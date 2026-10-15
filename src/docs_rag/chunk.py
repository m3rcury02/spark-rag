"""Section-aware chunking with content-addressed chunk IDs (D-009).

- Chunks never cross a section. The text a model embeds is `<breadcrumb>\\n\\n<body>`, the breadcrumb
  being the section path joined by " > "; `max_tokens` bounds that whole string, so the model sees exactly
  what was sized. Token counts are WordPiece counts in both embedding models' own units (docs_rag.tokens).
- A section's body is cut into units (paragraphs; code blocks kept whole), units are packed greedily, and a
  unit that does not fit is split by lines, then sentences, then at word-start tokens. Cuts land inside a
  word only when one word alone is over budget. Every chunk body is a verbatim slice of the section text, so
  no whitespace is invented at a cut; the one exception is a code block that has to be split, whose pieces
  are slices of its inner lines wrapped in its own fences.
- `overlap_tokens` of the previous chunk's tail (after whitespace; at a line start in code) open the next
  chunk, when they fit. Pieces of a split unit are sized to leave room for that tail. There is no overlap
  out of a whole code block or across the fence of a split one. Every chunk's `n_tokens` is a recount of its
  final embed text, never a sum.
- A one-line lead-in ending in a colon ("For example:", a "python:" tab label) is never the last line of a
  chunk that has more: it moves to the next chunk, with what it introduces.
- `chunk_id` hashes the doc path, section path, exact embed text and an occurrence counter (for identical
  texts in the same section). Nothing tag- or position-specific, so an unchanged section keeps its IDs in
  every tag, and any change to the embedded text (including the breadcrumb) gives new IDs.
"""

import hashlib
import json
import re
from dataclasses import dataclass

from docs_rag.clean import clean_page, fenced_blocks, residue
from docs_rag.sections import split_sections
from docs_rag.tokens import VOCAB_SHA256, count_tokens, token_spans

# Bump when chunk output changes for the same input; tests/test_chunk_golden.py enforces it.
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
    chunk_id: str


@dataclass(frozen=True)
class DocChunks:
    path: str
    title: str
    chunks: tuple[Chunk, ...]
    sections: int
    empty_sections: int  # headings with no body of their own (their text lives in their children's paths)
    residue: dict[str, int]  # markup left after cleaning; must be all zero


def breadcrumb(section_path: tuple[str, ...]) -> str:
    return BREADCRUMB_SEPARATOR.join(p for p in section_path if p)


def chunk_id(path: str, section_path: tuple[str, ...], embed_text: str, occurrence: int) -> str:
    key = json.dumps([path, list(section_path), embed_text, occurrence], ensure_ascii=False)  # length-safe
    return hashlib.sha256(key.encode()).hexdigest()[:32]


# --- splitting: every piece is a (start, end) slice of the section body ------------------------------------


@dataclass(frozen=True)
class _Piece:
    start: int
    end: int
    code: bool  # part of a fenced code block (whole or split)
    fence: tuple[str, str] | None = None  # (opener, closer) when the piece is part of a split code block
    block: int = -1  # the split code block the piece belongs to


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


_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z`(\[])")


def _split(body: str, start: int, end: int, budget: int) -> list[tuple[int, int]]:
    """Slices of at most `budget` tokens: the whole text if it fits, else by lines, sentences, word starts."""
    if count_tokens(body[start:end]) <= budget:
        return [(start, end)]
    lines = _lines(body, start, end)
    if len(lines) > 1:
        return [s for a, b in lines for s in _split(body, a, b, budget)]
    a, b = lines[0] if lines else (start, end)
    cuts = [m for m in _SENTENCE_END.finditer(body, a, b)]
    if cuts:
        bounds = [a, *(x for m in cuts for x in (m.start(), m.end())), b]
        return [s for i in range(0, len(bounds), 2) for s in _split(body, bounds[i], bounds[i + 1], budget)]
    return _cut_at_word_starts(body, a, b, budget)


def _pieces(body: str, budget: int, overlap: int) -> list[_Piece]:
    """Units that fit stay whole. A unit that has to be split is cut into slices that leave room for the
    overlap tail; a split code block's slices are its inner lines, to be re-fenced."""
    out: list[_Piece] = []
    for n, unit in enumerate(_units(body)):
        if count_tokens(body[unit.start : unit.end]) <= budget:
            out.append(unit)
        elif not unit.code:
            out += [_Piece(a, b, code=False) for a, b in _split(body, unit.start, unit.end, budget - overlap)]
        else:
            lines = _lines(body, unit.start, unit.end)
            fence = (body[lines[0][0] : lines[0][1]].strip(), body[lines[-1][0] : lines[-1][1]].strip())
            wrap = count_tokens(f"{fence[0]}\n{fence[1]}")
            if budget - overlap - wrap < 8:
                raise ValueError("max_tokens too small to hold a code fence")
            inner = _split(body, lines[1][0], lines[-2][1], budget - overlap - wrap) if len(lines) > 2 else []
            out += [_Piece(a, b, code=True, fence=fence, block=n) for a, b in inner]
    return out


# --- packing -----------------------------------------------------------------------------------------------


def _render(body: str, start: int, end: int, fence: tuple[str, str] | None) -> str:
    text = body[start:end]
    return f"{fence[0]}\n{text}\n{fence[1]}" if fence else text


def _tail_start(body: str, last: _Piece, chunk_start: int, overlap: int) -> int | None:
    """Where the overlap tail of a chunk ending in `last` starts: within its last `overlap` tokens, after the
    chunk start, after whitespace (prose) or at a line start (code). None when there is no such place."""
    if overlap <= 0 or (last.code and last.fence is None):
        return None  # never repeat part of a whole code block outside its fence
    spans = token_spans(body[last.start : last.end])
    for k in range(max(len(spans) - overlap, 0), len(spans)):
        pos = last.start + spans[k][0]
        if last.fence is not None:
            line_start = body.rfind("\n", 0, pos) + 1
            if body[line_start:pos].strip():
                continue
            pos = line_start
        if pos > chunk_start and body[pos - 1].isspace():
            return pos
    return None


def _is_lead_in(body: str, piece: _Piece) -> bool:
    """A one-line paragraph that ends with a colon, introducing what follows."""
    text = body[piece.start : piece.end]
    return not piece.code and "\n" not in text and text.rstrip().endswith(":")


def chunk_section(
    path: str, section_path: tuple[str, ...], body: str, config: ChunkConfig
) -> list[tuple[str, str]]:
    """(body, embed_text) of each chunk of one section. A chunk is a verbatim slice of the section body, or of
    one split code block's inner lines wrapped in that block's fences."""
    if not body.strip():
        return []
    crumb = breadcrumb(section_path)
    budget = config.max_tokens - count_tokens(crumb)
    if budget - config.overlap_tokens < 8:
        raise ValueError(f"breadcrumb of {path} > {crumb!r} leaves {budget} tokens of {config.max_tokens}")

    def fits(start: int, piece: _Piece) -> bool:
        return count_tokens(_render(body, start, piece.end, piece.fence)) <= budget

    chunks: list[tuple[int, int, tuple[str, str] | None]] = []
    start = 0
    current: list[_Piece] = []  # pieces of the chunk being built; it starts at `start`
    for piece in _pieces(body, budget, config.overlap_tokens):
        if current:
            last = current[-1]
            same_group = piece.fence == last.fence and piece.block == last.block
            if same_group and fits(start, piece):
                current.append(piece)
                continue
            if same_group and len(current) > 1 and _is_lead_in(body, last) and fits(last.start, piece):
                # "For example:" or a "python:" tab label goes with what it introduces, not what precedes it
                chunks.append((start, current[-2].end, current[-2].fence))
                start, current = last.start, [last, piece]
                continue
            chunks.append((start, last.end, last.fence))
            tail = _tail_start(body, last, start, config.overlap_tokens) if same_group else None
            if tail is not None and fits(tail, piece):
                start, current = tail, [piece]
                continue
        start, current = piece.start, [piece]
    if current:
        chunks.append((start, current[-1].end, current[-1].fence))
    bodies = [_render(body, s, e, fence).strip() for s, e, fence in chunks]
    return [(b, f"{crumb}\n\n{b}") for b in bodies]


def chunk_document(path: str, raw_markdown: str, config: ChunkConfig) -> DocChunks:
    """Clean one page, split it into sections, chunk every section with content."""
    page = clean_page(raw_markdown, fallback_title=path.rsplit("/", 1)[-1].removesuffix(".md"))
    sections = split_sections(page.title, page.text)
    chunks: list[Chunk] = []
    seen: dict[tuple[tuple[str, ...], str], int] = {}
    for section in sections:
        for body, embed_text in chunk_section(path, section.path, section.body, config):
            n_tokens = count_tokens(embed_text)
            if n_tokens > config.max_tokens:  # a bug, not a data problem: packing is supposed to prevent it
                raise AssertionError(f"chunk of {n_tokens} tokens > max {config.max_tokens} in {path}")
            occurrence = seen.get((section.path, embed_text), 0)
            seen[(section.path, embed_text)] = occurrence + 1
            chunks.append(
                Chunk(
                    path=path,
                    section_path=section.path,
                    chunk_index=len(chunks),
                    body=body,
                    embed_text=embed_text,
                    n_tokens=n_tokens,
                    chunk_id=chunk_id(path, section.path, embed_text, occurrence),
                )
            )
    return DocChunks(
        path=path,
        title=page.title,
        chunks=tuple(chunks),
        sections=len(sections),
        empty_sections=sum(1 for s in sections if not s.body),
        residue=residue(page.text),
    )
