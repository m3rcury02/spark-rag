"""Section-aware chunking with content-addressed chunk IDs (D-009).

- Chunks never cross a section. The text a model embeds is `<breadcrumb>\\n\\n<body>`, the breadcrumb
  being the section path joined by " > "; `max_tokens` bounds that whole string, so the model sees exactly
  what was sized. Token counts are WordPiece counts in both embedding models' own units (docs_rag.tokens).
- A section's body is cut into units (paragraphs, GFM tables, code blocks), units are packed greedily, and a
  unit that does not fit is split: prose at sentence ends first (its line breaks are only the source's hard
  wrapping), a list at its item starts, code at its lines; then at lines, sentences and finally word-start
  tokens. Cuts land inside a word only when one word alone is over budget.
- Every chunk body is a verbatim slice of the cleaned section text, so no whitespace is invented at a cut,
  except inside a *frame*: a code block that has to be split gives slices of its inner lines, each wrapped in
  the block's own fences; a table that has to be split gives one chunk per row, each under the table's
  header row (and a long row's later pieces also under the row's first cell, its property name). A table row
  is never packed with anything else. Each chunk carries its frame, so `body == frame_head + slice +
  frame_tail` exactly.
- `overlap_tokens` of the previous chunk's tail (after whitespace; at a line start in code) open the next
  chunk, when they fit. Pieces of a split unit are sized to leave room for that tail. There is no overlap
  out of a whole code block, or from one frame (code block, table row) into anything else. Every chunk's
  `n_tokens` is a recount of its final embed text, never a sum.
- Each chunk records `char_start`/`char_end`: where its verbatim slice sits in the cleaned page text
  (CLEANER_VERSION names that text), so an M6 answer quote can be matched to every chunk that holds it.
- `chunk_id` hashes the doc path, section path, exact embed text and an occurrence counter (for identical
  texts in the same section). Nothing tag- or position-specific, so an unchanged section keeps its IDs in
  every tag, and any change to the embedded text (including the breadcrumb) gives new IDs.
"""

import hashlib
import json
import re
from dataclasses import dataclass

from docs_rag.clean import CLEANER_VERSION, DELIMITER_ROW, clean_page, fenced_blocks
from docs_rag.sections import split_sections
from docs_rag.tokens import VOCAB_SHA256, count_tokens, token_spans

# Bump when chunk output changes for the same input. The goldens (tests/test_chunk_golden.py on fixtures,
# scripts/corpus_fingerprint.py on the real corpus) fail on any change and are never overwritten.
CHUNKER_VERSION = "1"
BREADCRUMB_SEPARATOR = " > "


@dataclass(frozen=True)
class ChunkConfig:
    max_tokens: int = 254  # MiniLM's 256-token input limit minus [CLS] and [SEP]
    overlap_tokens: int = 32

    def __post_init__(self) -> None:
        if not 16 <= self.max_tokens <= 510:
            raise ValueError(f"max_tokens must be in 16..510, got {self.max_tokens}")
        if not 0 <= self.overlap_tokens < self.max_tokens // 2:
            raise ValueError(
                f"overlap_tokens must be in 0..{self.max_tokens // 2 - 1}, got {self.overlap_tokens}"
            )

    @property
    def config_id(self) -> str:
        """Identifies everything that shapes the output; part of the silver key, not of chunk IDs."""
        spec = {
            "cleaner": CLEANER_VERSION,
            "chunker": CHUNKER_VERSION,
            "max": self.max_tokens,
            "overlap": self.overlap_tokens,
            "vocab": VOCAB_SHA256,
        }
        return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class Chunk:
    path: str
    section_path: tuple[str, ...]
    chunk_index: int  # position in the doc; for ordering only, not identity
    body: str
    embed_text: str
    n_tokens: int
    occurrence: int  # how many earlier chunks of this doc have the same section path and embed text
    chunk_id: str
    char_start: int  # the chunk's verbatim slice is clean_text[char_start:char_end] (without any frame)
    char_end: int
    frame: str  # "none" (the body is the slice), "code" (inside its block's fences), "table" (under a header)
    frame_head: str  # body == frame_head + clean_text[char_start:char_end] + frame_tail; not stored in silver
    frame_tail: str


def rebuilds(chunk: Chunk, clean_text: str) -> bool:
    """Whether the chunk's body is exactly its frame around its slice of the cleaned page text."""
    return chunk.frame_head + clean_text[chunk.char_start : chunk.char_end] + chunk.frame_tail == chunk.body


@dataclass(frozen=True)
class DocChunks:
    path: str
    title: str
    chunks: tuple[Chunk, ...]
    sections: int
    empty_sections: int  # headings with no body of their own (their text lives in their children's paths)
    residue: dict[str, int]  # markup left after cleaning; must be all zero
    clean_sha256: str  # of the cleaned page text the chunk offsets point into


def breadcrumb(section_path: tuple[str, ...]) -> str:
    return BREADCRUMB_SEPARATOR.join(p for p in section_path if p)


def chunk_id(path: str, section_path: tuple[str, ...], embed_text: str, occurrence: int) -> str:
    key = json.dumps([path, list(section_path), embed_text, occurrence], ensure_ascii=False)  # length-safe
    return hashlib.sha256(key.encode()).hexdigest()[:32]


# --- splitting: every piece is a (start, end) slice of the section body ------------------------------------


@dataclass(frozen=True)
class _Frame:
    """Text around the pieces of one split block: a code block's fences, or a table's header over one row.
    Pieces of different frames never share a chunk."""

    group: int
    start: int  # where the framed content starts in the section body
    head: str  # prefix of a chunk that starts at `start`
    head_cont: str  # prefix of a chunk that starts later in the content
    tail: str  # suffix
    code: bool  # overlap tails start at a line start


@dataclass(frozen=True)
class _Piece:
    start: int
    end: int
    code: bool  # part of a fenced code block (whole or split)
    frame: _Frame | None = None


def _lines(body: str, start: int, end: int) -> list[tuple[int, int]]:
    """(start, end) of every non-blank line in body[start:end], leading whitespace kept (code indentation)."""
    spans = []
    pos = start
    for line in body[start:end].split("\n"):
        if line.strip():
            spans.append((pos, pos + len(line.rstrip())))
        pos += len(line) + 1
    return spans


def _units(body: str) -> list[_Piece]:
    """Paragraphs (runs of non-blank lines) and fenced code blocks, which stay one unit across blank lines."""
    offsets = [0]
    for line in body.split("\n"):
        offsets.append(offsets[-1] + len(line) + 1)
    lines = body.split("\n")
    blocks = dict(fenced_blocks(lines))
    units: list[_Piece] = []
    first = None
    i = 0
    while i <= len(lines):
        if i == len(lines) or i in blocks or not lines[i].strip():
            if first is not None:
                units.append(_Piece(offsets[first], offsets[i] - 1, code=False))
                first = None
            if i in blocks:
                units.append(_Piece(offsets[i], offsets[blocks[i] + 1] - 1, code=True))
                i = blocks[i]
        elif first is None:
            first = i
        i += 1
    return units


def _word_start(text: str, char: int) -> bool:
    return char == 0 or not (text[char - 1].isalnum() and text[char].isalnum())


def _cut_at_word_starts(body: str, start: int, end: int, budget: int) -> list[tuple[int, int]]:
    """Cut body[start:end] into slices of at most `budget` tokens where a token starts a word. Only a single
    word longer than the budget is cut inside; those slices are recounted, since a word fragment can tokenise
    differently on its own."""
    spans = [(start + a, start + b) for a, b in token_spans(body[start:end])]

    def end_of(k: int) -> int:
        return spans[k][0] if k < len(spans) else end

    out: list[tuple[int, int]] = []
    k = 0
    while k < len(spans):
        stop = min(k + budget, len(spans))
        if stop < len(spans):
            cut = stop
            while cut > k + 1 and not _word_start(body, spans[cut][0]):
                cut -= 1
            if _word_start(body, spans[cut][0]):
                stop = cut
        while stop > k + 1 and count_tokens(body[spans[k][0] : end_of(stop)]) > budget:
            stop -= 1
        piece_end = end_of(stop)
        while piece_end > spans[k][0] and body[piece_end - 1].isspace():
            piece_end -= 1
        out.append((spans[k][0], piece_end))
        k = stop
    return out


# A sentence ends at . ! or ? before whitespace and a capital, code or bracket, but not after "e.g." / "i.e.".
_SENTENCE_END = re.compile(r"(?<=[.!?])(?<!\b[eE]\.g\.)(?<!\b[iI]\.e\.)\s+(?=[A-Z`(\[])")


# A line that starts a list item or a table row: such a paragraph is cut at its lines, not its sentences.
_STRUCTURED_LINE = re.compile(r"\s*(?:[-*+]|\d+[.)])\s|\s*\|")


def _split(body: str, start: int, end: int, budget: int, by_lines: bool = False) -> list[tuple[int, int]]:
    """Slices of at most `budget` tokens: the whole text if it fits. Otherwise prose is cut at sentence ends
    first (its line breaks are only the source's hard wrapping), a list or a table-like paragraph at the lines
    that start an item (a hard-wrapped item stays together), code (`by_lines`) at its lines; then lines,
    sentences and finally word starts."""
    if count_tokens(body[start:end]) <= budget:
        return [(start, end)]
    lines = _lines(body, start, end)
    a, b = (lines[0][0], lines[-1][1]) if lines else (start, end)
    items = [k for k, (x, y) in enumerate(lines) if k and _STRUCTURED_LINE.match(body[x:y])]
    if not by_lines and not items:
        cuts = list(_SENTENCE_END.finditer(body, a, b))
        if cuts:
            bounds = [a, *(x for m in cuts for x in (m.start(), m.end())), b]
            pairs = range(0, len(bounds), 2)
            return [s for i in pairs for s in _split(body, bounds[i], bounds[i + 1], budget, by_lines=True)]
    if not by_lines and items:
        starts = [0, *items, len(lines)]
        spans = [(lines[i][0], lines[j - 1][1]) for i, j in zip(starts, starts[1:], strict=False)]
        return [s for x, y in spans for s in _split(body, x, y, budget)]
    if len(lines) > 1:
        return [s for x, y in lines for s in _split(body, x, y, budget)]
    cuts = list(_SENTENCE_END.finditer(body, a, b))
    if cuts:
        bounds = [a, *(x for m in cuts for x in (m.start(), m.end())), b]
        return [s for i in range(0, len(bounds), 2) for s in _split(body, bounds[i], bounds[i + 1], budget)]
    return _cut_at_word_starts(body, a, b, budget)


def _table_rows(body: str, unit: _Piece) -> tuple[str, list[tuple[int, int]]] | None:
    """(header line, data row spans) when the unit is a GFM table: a header row, a delimiter row, rows."""
    lines = _lines(body, unit.start, unit.end)
    if (
        len(lines) < 3
        or "|" not in body[slice(*lines[0])]
        or not DELIMITER_ROW.fullmatch(body[slice(*lines[1])])
    ):
        return None
    return body[slice(*lines[0])].strip(), lines[2:]


def _first_cell(row: str, header: str) -> str:
    """The row's first cell ("" when empty); a leading pipe is a border when the header has one."""
    text = row.strip()
    if header.startswith("|"):
        text = text.removeprefix("|")
    return text.split("|", 1)[0].strip()


def _pieces(body: str, budget: int, overlap: int) -> list[_Piece]:
    """Units that fit stay whole. A unit that has to be split is cut into slices that leave room for the
    overlap tail: a code block into its inner lines (re-fenced), a table into its rows (each under the
    header), anything else by `_split`."""
    out: list[_Piece] = []
    groups = 0
    for unit in _units(body):
        if count_tokens(body[unit.start : unit.end]) <= budget:
            out.append(unit)
            continue
        table = None if unit.code else _table_rows(body, unit)
        if unit.code:
            lines = _lines(body, unit.start, unit.end)
            opener, closer = body[slice(*lines[0])].strip(), body[slice(*lines[-1])].strip()
            if len(lines) <= 2:  # nothing inside the fences, so its opener alone is over budget
                raise ValueError(f"code fence over budget: {opener[:60]!r}")
            groups += 1
            frame = _Frame(groups, lines[1][0], f"{opener}\n", f"{opener}\n", f"\n{closer}", code=True)
            room = budget - overlap - count_tokens(f"{opener}\n{closer}")
            if room < 8:
                raise ValueError("max_tokens too small to hold a code fence")
            inner = _split(body, lines[1][0], lines[-2][1], room, by_lines=True)
            out += [_Piece(a, b, True, frame) for a, b in inner]
        elif table:
            header, rows = table
            for a, b in rows:
                groups += 1
                cell = _first_cell(body[a:b], header)
                cont = f"{header}\n{cell} | " if cell else f"{header}\n"
                frame = _Frame(groups, a, f"{header}\n", cont, "", code=False)
                if count_tokens(f"{header}\n{body[a:b]}") <= budget:
                    out.append(_Piece(a, b, False, frame))
                    continue
                room = budget - overlap - count_tokens(cont)
                if room < 8:
                    raise ValueError("max_tokens too small to hold a table header")
                out += [_Piece(x, y, False, frame) for x, y in _split(body, a, b, room)]
        else:
            out += [_Piece(a, b, False) for a, b in _split(body, unit.start, unit.end, budget - overlap)]
    return out


# --- packing -----------------------------------------------------------------------------------------------


def _around(start: int, frame: _Frame | None) -> tuple[str, str]:
    """The (head, tail) a slice starting at `start` is wrapped in."""
    if frame is None:
        return "", ""
    return (frame.head if start == frame.start else frame.head_cont), frame.tail


def _render(body: str, start: int, end: int, frame: _Frame | None) -> str:
    head, tail = _around(start, frame)
    return head + body[start:end] + tail


def _tail_start(body: str, last: _Piece, chunk_start: int, overlap: int) -> int | None:
    """Where the overlap tail of a chunk ending in `last` starts: within its last `overlap` tokens, after the
    chunk start, after whitespace (prose) or at a line start (code). None when there is no such place."""
    if overlap <= 0 or (last.code and last.frame is None):
        return None  # never repeat part of a whole code block outside its fence
    spans = token_spans(body[last.start : last.end])
    for k in range(max(len(spans) - overlap, 0), len(spans)):
        pos = last.start + spans[k][0]
        if last.frame is not None and last.frame.code:
            line_start = body.rfind("\n", 0, pos) + 1
            if body[line_start:pos].strip():
                continue
            pos = line_start
        if pos > chunk_start and body[pos - 1].isspace():
            return pos
    return None


@dataclass(frozen=True)
class SectionChunk:
    body: str  # == frame_head + section_body[start:end] + frame_tail
    embed_text: str
    start: int
    end: int
    frame: str  # "none", "code" or "table"
    frame_head: str
    frame_tail: str


def chunk_section(
    path: str, section_path: tuple[str, ...], body: str, config: ChunkConfig
) -> list[SectionChunk]:
    """The chunks of one section, in order."""
    if not body.strip():
        return []
    crumb = breadcrumb(section_path)
    budget = config.max_tokens - count_tokens(crumb)
    if budget - config.overlap_tokens < 8:
        raise ValueError(f"breadcrumb of {path} > {crumb!r} leaves {budget} tokens of {config.max_tokens}")

    def fits(start: int, piece: _Piece) -> bool:
        return count_tokens(_render(body, start, piece.end, piece.frame)) <= budget

    spans: list[tuple[int, int, _Frame | None]] = []
    start = 0
    last: _Piece | None = None  # last piece of the chunk being built; the chunk starts at `start`
    for piece in _pieces(body, budget, config.overlap_tokens):
        if last is not None:
            same_group = piece.frame == last.frame  # both unframed, or both in the same frame
            if same_group and fits(start, piece):
                last = piece
                continue
            spans.append((start, last.end, last.frame))
            tail = _tail_start(body, last, start, config.overlap_tokens) if same_group else None
            if tail is not None and fits(tail, piece):
                start, last = tail, piece
                continue
        start, last = piece.start, piece
    if last is not None:
        spans.append((start, last.end, last.frame))
    out = []
    for s, e, frame in spans:
        if frame is None:  # trim the slice itself, so its offsets match the body exactly
            s += len(body[s:e]) - len(body[s:e].lstrip())
            e -= len(body[s:e]) - len(body[s:e].rstrip())
        frame_head, frame_tail = _around(
            s, frame
        )  # a framed slice keeps its edges: code indentation is content
        text = frame_head + body[s:e] + frame_tail
        kind = "none" if frame is None else "code" if frame.code else "table"
        out.append(SectionChunk(text, f"{crumb}\n\n{text}", s, e, kind, frame_head, frame_tail))
    return out


def chunk_document(path: str, raw_markdown: str, config: ChunkConfig) -> DocChunks:
    """Clean one page, split it into sections, chunk every section with content."""
    page = clean_page(raw_markdown, fallback_title=path.rsplit("/", 1)[-1].removesuffix(".md"))
    sections = split_sections(page.title, page.text)
    chunks: list[Chunk] = []
    seen: dict[tuple[tuple[str, ...], str], int] = {}
    for section in sections:
        for sc in chunk_section(path, section.path, section.body, config):
            n_tokens = count_tokens(sc.embed_text)
            if n_tokens > config.max_tokens:  # a bug, not a data problem: packing is supposed to prevent it
                raise AssertionError(f"chunk of {n_tokens} tokens > max {config.max_tokens} in {path}")
            start, end = section.start + sc.start, section.start + sc.end
            if (
                sc.frame_head + page.text[start:end] + sc.frame_tail != sc.body
            ):  # the offsets contract, every run
                raise AssertionError(f"chunk body does not rebuild from its offsets in {path}")
            occurrence = seen.get((section.path, sc.embed_text), 0)
            seen[(section.path, sc.embed_text)] = occurrence + 1
            chunks.append(
                Chunk(
                    path=path,
                    section_path=section.path,
                    chunk_index=len(chunks),
                    body=sc.body,
                    embed_text=sc.embed_text,
                    n_tokens=n_tokens,
                    occurrence=occurrence,
                    chunk_id=chunk_id(path, section.path, sc.embed_text, occurrence),
                    char_start=start,
                    char_end=end,
                    frame=sc.frame,
                    frame_head=sc.frame_head,
                    frame_tail=sc.frame_tail,
                )
            )
    return DocChunks(
        path=path,
        title=page.title,
        chunks=tuple(chunks),
        sections=len(sections),
        empty_sections=sum(1 for s in sections if not s.body),
        residue=page.residue,
        clean_sha256=hashlib.sha256(page.text.encode()).hexdigest(),
    )
