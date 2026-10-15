"""Section-aware chunking with content-addressed chunk IDs (D-009).

- Chunks never cross a section. The text a model embeds is `<breadcrumb>\\n\\n<body>`, the breadcrumb
  being the section path joined by " > "; `max_tokens` bounds that whole string, so the model sees exactly
  what was sized. Token counts are WordPiece counts in both embedding models' own units (docs_rag.tokens).
- A section's body is cut into units (paragraphs; code blocks kept whole), units are packed greedily, and a
  unit that does not fit is split by lines, then sentences, then at word-start tokens. A code block that has
  to be split is re-fenced piece by piece. Cuts never land inside a word, where re-tokenising would change the
  count.
- `overlap_tokens` of the previous chunk's tail (cut at a word start) open the next chunk in the same
  section, when they fit. Every chunk's `n_tokens` is a recount of its final embed text, never a sum.
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


# --- splitting ---------------------------------------------------------------------------------------------


def _units(body: str) -> list[str]:
    """Paragraph-like units separated by blank lines. A fenced code block is one unit, blank lines and all."""
    lines = body.split("\n")
    units: list[str] = []
    current: list[str] = []
    i = 0
    blocks = dict(fenced_blocks(lines))
    while i < len(lines):
        if i in blocks:
            if current:
                units.append("\n".join(current))
                current = []
            units.append("\n".join(lines[i : blocks[i] + 1]))
            i = blocks[i] + 1
            continue
        if lines[i].strip():
            current.append(lines[i])
        elif current:
            units.append("\n".join(current))
            current = []
        i += 1
    if current:
        units.append("\n".join(current))
    return [u.strip("\n") for u in units if u.strip()]


def _cut_at_word_starts(text: str, budget: int) -> list[str]:
    """Split text into pieces of at most `budget` tokens, cutting only where a token starts a word."""
    spans = token_spans(text)
    pieces: list[str] = []
    start_token = 0
    while start_token < len(spans):
        end_token = min(start_token + budget, len(spans))
        if end_token < len(spans):
            cut = end_token
            while cut > start_token + 1 and not _word_start(text, spans[cut][0]):
                cut -= 1
            end_token = cut if cut > start_token + 1 or _word_start(text, spans[cut][0]) else end_token
        piece = text[
            spans[start_token][0] : spans[end_token][0] if end_token < len(spans) else len(text)
        ].strip()
        if piece:
            pieces.append(piece)
        start_token = end_token
    return pieces


def _word_start(text: str, char: int) -> bool:
    return char == 0 or not (text[char - 1].isalnum() and text[char].isalnum())


def _split_unit(unit: str, budget: int) -> list[str]:
    """A unit that fits stays whole. Otherwise: lines, then sentences, then word-start token cuts."""
    if count_tokens(unit) <= budget:
        return [unit]
    lines = unit.split("\n")
    fence = re.match(r"^\s*(`{3,}|~{3,})(.*)$", lines[0])
    if fence and len(lines) >= 2:  # a fenced code block: split its inner lines, re-fence every piece
        opener, closer = lines[0].strip(), fence.group(1)
        wrap = count_tokens(f"{opener}\n{closer}")
        if wrap + 1 >= budget:
            raise ValueError("max_tokens too small to hold a code fence")
        inner = _pack(
            [p for line in lines[1:-1] for p in _split_unit(line, budget - wrap)], budget - wrap, "\n"
        )
        return [f"{opener}\n{piece}\n{closer}" for piece in inner]
    if len(lines) > 1:
        return _pack([p for line in lines for p in _split_unit(line, budget)], budget, "\n")
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z`(\[])", unit)
    if len(sentences) > 1:
        return _pack([p for s in sentences for p in _split_unit(s, budget)], budget, " ")
    return _cut_at_word_starts(unit, budget)


def _pack(pieces: list[str], budget: int, joiner: str) -> list[str]:
    """Greedily join consecutive pieces while the recounted total stays within budget."""
    out: list[str] = []
    for piece in pieces:
        if out and count_tokens(out[-1] + joiner + piece) <= budget:
            out[-1] = out[-1] + joiner + piece
        else:
            out.append(piece)
    return out


def _tail(text: str, overlap: int) -> str:
    """The last `overlap` tokens of text (or fewer), starting at a word start."""
    if overlap <= 0:
        return ""
    spans = token_spans(text)
    if len(spans) <= overlap:
        return ""  # the whole previous chunk would repeat; no overlap rather than a duplicate
    i = len(spans) - overlap
    while i < len(spans) and not _word_start(text, spans[i][0]):
        i += 1
    return text[spans[i][0] :].strip() if i < len(spans) else ""


# --- chunking ----------------------------------------------------------------------------------------------


def chunk_section(
    path: str, section_path: tuple[str, ...], body: str, config: ChunkConfig
) -> list[tuple[str, str]]:
    """(body, embed_text) of each chunk of one section."""
    if not body.strip():
        return []
    crumb = breadcrumb(section_path)
    budget = config.max_tokens - count_tokens(crumb)
    if budget < 8:
        raise ValueError(f"breadcrumb of {path} > {crumb!r} leaves {budget} tokens of {config.max_tokens}")
    pieces = [p for u in _units(body) for p in _split_unit(u, budget)]
    bodies: list[str] = []
    current = ""
    for piece in pieces:
        candidate = f"{current}\n\n{piece}" if current else piece
        if current and count_tokens(candidate) > budget:
            bodies.append(current)
            tail = _tail(current, config.overlap_tokens)
            with_tail = f"{tail}\n\n{piece}" if tail else piece
            current = with_tail if tail and count_tokens(with_tail) <= budget else piece
        else:
            current = candidate
    if current:
        bodies.append(current)
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
