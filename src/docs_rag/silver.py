"""Silver layer contract: the silver_chunks columns, the records written to it, and the checks around them.

Silver holds one immutable chunking per (config_id, source_repo, version_tag): the chunks of every bronze doc
of that tag, chunked with that config (D-010). Like bronze, a stored chunking is never rewritten. Chunking a
stored tag again must reproduce it exactly (same commit, same (path, chunk_id) rows); anything else means the
chunker changed without a CHUNKER_VERSION bump or bronze changed underneath, and the run stops.

Every stored row is self-checking: its chunk_id is recomputed from its own path, section path, embed text and
occurrence, and its n_tokens from its embed text, on every run (bronze's sha2 recheck, applied to chunks).
"""

import dataclasses
import datetime as dt
import hashlib
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig, DocChunks, breadcrumb, chunk_document, chunk_id
from docs_rag.clean import CLEANER_VERSION
from docs_rag.tokens import count_tokens

# Column order here is the table's column order and the field order of SilverRecord (a test enforces both).
SILVER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("config_id", "STRING NOT NULL"),
    ("source_repo", "STRING NOT NULL"),
    ("version_tag", "STRING NOT NULL"),
    ("commit_sha", "STRING NOT NULL"),
    ("path", "STRING NOT NULL"),
    ("doc_content_sha256", "STRING NOT NULL"),  # bronze content_sha256 of the doc the chunk came from
    ("doc_clean_sha256", "STRING NOT NULL"),  # sha256 of the cleaned doc text the offsets point into
    ("chunk_id", "STRING NOT NULL"),
    ("occurrence", "INT NOT NULL"),
    ("chunk_index", "INT NOT NULL"),
    ("section_path", "ARRAY<STRING> NOT NULL"),  # (doc title, heading, subheading, ...)
    ("body", "STRING NOT NULL"),
    ("embed_text", "STRING NOT NULL"),
    ("n_tokens", "INT NOT NULL"),
    ("char_start", "INT NOT NULL"),  # the chunk's verbatim slice of the cleaned doc text
    ("char_end", "INT NOT NULL"),
    ("cleaner_version", "STRING NOT NULL"),
    ("chunker_version", "STRING NOT NULL"),
    ("max_tokens", "INT NOT NULL"),
    ("overlap_tokens", "INT NOT NULL"),
    ("chunked_at", "TIMESTAMP NOT NULL"),
    ("chunk_run_id", "STRING NOT NULL"),
)
MAX_MESSAGES = 20  # violations listed per check; the count is always complete


def silver_schema_ddl() -> str:
    return ", ".join(f"{name} {sql_type}" for name, sql_type in SILVER_COLUMNS)


@dataclass(frozen=True)
class BronzeDoc:
    """One bronze row as silver reads it."""

    path: str
    content: str
    content_sha256: str  # as stored
    content_sha256_actual: str  # sha2(content, 256) recomputed when read
    commit_sha: str


@dataclass(frozen=True)
class SilverRecord:
    config_id: str
    source_repo: str
    version_tag: str
    commit_sha: str
    path: str
    doc_content_sha256: str
    doc_clean_sha256: str
    chunk_id: str
    occurrence: int
    chunk_index: int
    section_path: tuple[str, ...]
    body: str
    embed_text: str
    n_tokens: int
    char_start: int
    char_end: int
    cleaner_version: str
    chunker_version: str
    max_tokens: int
    overlap_tokens: int
    chunked_at: dt.datetime
    chunk_run_id: str

    def as_row(self) -> tuple[object, ...]:
        row = dataclasses.astuple(self)
        return tuple(list(v) if isinstance(v, tuple) else v for v in row)  # ARRAY<STRING> wants a list


def _capped(problems: list[str]) -> list[str]:
    return problems[:MAX_MESSAGES] + (
        [f"... {len(problems) - MAX_MESSAGES} more"] if len(problems) > 20 else []
    )


def bronze_violations(docs: Sequence[BronzeDoc]) -> list[str]:
    """The bronze snapshot silver is about to chunk must be whole: rows present, one commit, unique paths,
    every stored hash matching its content."""
    problems = []
    if not docs:
        problems.append("no bronze rows for the tag")
    commits = {d.commit_sha for d in docs}
    if len(commits) > 1:
        problems.append(f"{len(commits)} commits in one tag: {sorted(commits)}")
    dupes = [p for p, n in Counter(d.path for d in docs).items() if n > 1]
    if dupes:
        problems.append(f"{len(dupes)} paths stored more than once, e.g. {dupes[:3]}")
    bad = [d.path for d in docs if d.content_sha256 != d.content_sha256_actual]
    if bad:
        problems.append(f"{len(bad)} rows whose content does not match content_sha256, e.g. {bad[:3]}")
    return problems


def chunk_docs(docs: Sequence[BronzeDoc], config: ChunkConfig) -> list[DocChunks]:
    return [chunk_document(d.path, d.content, config) for d in sorted(docs, key=lambda d: d.path)]


def to_records(
    docs: Sequence[BronzeDoc],
    chunked: Sequence[DocChunks],
    source_repo: str,
    version_tag: str,
    config: ChunkConfig,
    run_id: str,
    chunked_at: dt.datetime,
) -> list[SilverRecord]:
    by_path = {d.path: d for d in docs}
    return [
        SilverRecord(
            config_id=config.config_id,
            source_repo=source_repo,
            version_tag=version_tag,
            commit_sha=by_path[doc.path].commit_sha,
            path=doc.path,
            doc_content_sha256=by_path[doc.path].content_sha256,
            doc_clean_sha256=doc.clean_sha256,
            chunk_id=c.chunk_id,
            occurrence=c.occurrence,
            chunk_index=c.chunk_index,
            section_path=c.section_path,
            body=c.body,
            embed_text=c.embed_text,
            n_tokens=c.n_tokens,
            char_start=c.char_start,
            char_end=c.char_end,
            cleaner_version=CLEANER_VERSION,
            chunker_version=CHUNKER_VERSION,
            max_tokens=config.max_tokens,
            overlap_tokens=config.overlap_tokens,
            chunked_at=chunked_at,
            chunk_run_id=run_id,
        )
        for doc in chunked
        for c in doc.chunks
    ]


def record_violations(
    records: Sequence[SilverRecord],
    chunked: Sequence[DocChunks],
    config: ChunkConfig,
    zero_chunk_docs: frozenset[str],
) -> list[str]:
    """Checks on a freshly computed chunking, before anything is written."""
    problems = []
    over = [r.chunk_id for r in records if r.n_tokens > config.max_tokens]
    if over:
        problems.append(f"{len(over)} chunks over {config.max_tokens} tokens")
    empty = [r.chunk_id for r in records if not r.body.strip()]
    if empty:
        problems.append(f"{len(empty)} chunks with an empty body")
    dupes = [k for k, n in Counter(r.chunk_id for r in records).items() if n > 1]
    if dupes:
        problems.append(f"{len(dupes)} chunk_ids used more than once, e.g. {dupes[:3]}")
    residue = [f"{d.path}: {d.residue}" for d in chunked if any(d.residue.values())]
    if residue:
        problems.append(f"{len(residue)} docs with markup left after cleaning: {_capped(residue)}")
    unexpected = sorted(d.path for d in chunked if not d.chunks and d.path not in zero_chunk_docs)
    if unexpected:
        problems.append(
            f"{len(unexpected)} docs with no chunks that are not expected to be empty: {unexpected}"
        )
    return problems


@dataclass(frozen=True)
class StoredChunk:
    """A silver row as read back for re-verification."""

    path: str
    commit_sha: str
    doc_content_sha256: str
    doc_clean_sha256: str
    chunk_id: str
    occurrence: int
    chunk_index: int
    section_path: tuple[str, ...]
    body: str
    embed_text: str
    n_tokens: int
    char_start: int
    char_end: int
    cleaner_version: str
    chunker_version: str
    max_tokens: int
    overlap_tokens: int


def stored_violations(rows: Sequence[StoredChunk], config: ChunkConfig) -> list[str]:
    """Every stored row must still be what its chunk_id says it is."""
    problems = []
    for r in rows:
        where = f"{r.path} #{r.chunk_index}"
        if chunk_id(r.path, r.section_path, r.embed_text, r.occurrence) != r.chunk_id:
            problems.append(f"{where}: chunk_id does not match its content")
        if r.embed_text != f"{breadcrumb(r.section_path)}\n\n{r.body}":
            problems.append(f"{where}: embed_text is not breadcrumb + body")
        if count_tokens(r.embed_text) != r.n_tokens or r.n_tokens > config.max_tokens:
            problems.append(f"{where}: n_tokens {r.n_tokens} is wrong or over {config.max_tokens}")
        if not 0 <= r.char_start < r.char_end or r.char_end - r.char_start > len(r.body):
            problems.append(f"{where}: offsets {r.char_start}..{r.char_end} do not fit its body")
        if (r.cleaner_version, r.chunker_version, r.max_tokens, r.overlap_tokens) != (
            CLEANER_VERSION,
            CHUNKER_VERSION,
            config.max_tokens,
            config.overlap_tokens,
        ):
            problems.append(f"{where}: stored under another cleaner/chunker version or config")
    dupes = [k for k, n in Counter(r.chunk_id for r in rows).items() if n > 1]
    if dupes:
        problems.append(f"{len(dupes)} chunk_ids stored more than once, e.g. {dupes[:3]}")
    return _capped(problems)


def _identity(r: "SilverRecord | StoredChunk") -> tuple[object, ...]:
    return (
        r.path,
        r.chunk_id,
        r.chunk_index,
        r.char_start,
        r.char_end,
        r.commit_sha,
        r.doc_content_sha256,
        r.doc_clean_sha256,
    )


@dataclass(frozen=True)
class StoredCheck:
    status: Literal["new", "identical", "conflict"]
    stored_chunks: int
    computed_chunks: int
    only_stored: int  # rows stored that the fresh chunking does not produce
    only_computed: int  # rows the fresh chunking produces that are not stored


def check_against_stored(records: Sequence[SilverRecord], stored: Sequence[StoredChunk]) -> StoredCheck:
    """'identical' means the same multiset of (path, chunk_id, chunk_index, offsets, commit, doc hashes)."""
    new = Counter(_identity(r) for r in records)
    old = Counter(_identity(r) for r in stored)
    if not old:
        status: Literal["new", "identical", "conflict"] = "new"
    else:
        status = "identical" if new == old else "conflict"
    return StoredCheck(
        status=status,
        stored_chunks=sum(old.values()),
        computed_chunks=sum(new.values()),
        only_stored=sum((old - new).values()),
        only_computed=sum((new - old).values()),
    )


def fingerprint(pairs: Iterable[tuple[str, str]]) -> str:
    """sha256 of the sorted (path, chunk_id) lines: one value to compare a chunking across machines."""
    lines = sorted(f"{path}\t{cid}\n" for path, cid in pairs)
    return hashlib.sha256("".join(lines).encode()).hexdigest()


def token_stats(records: Sequence[SilverRecord], chunked: Sequence[DocChunks]) -> dict[str, object]:
    """Distribution numbers for the run summary, so later thresholds (M4) come from a saved run."""
    n = sorted(r.n_tokens for r in records)
    body = [count_tokens(r.body) for r in records]
    edges = [0, 16, 32, 64, 128, 192, 224, 254]
    hist = {f"{a + 1}-{b}": sum(a < x <= b for x in n) for a, b in zip(edges, edges[1:], strict=False)}
    return {
        "chunks": len(n),
        "n_tokens_min": n[0] if n else None,
        "n_tokens_median": n[len(n) // 2] if n else None,
        "n_tokens_max": n[-1] if n else None,
        "n_tokens_histogram": hist,
        "body_tokens_under": {str(k): sum(x < k for x in body) for k in (5, 10, 20)},
        "docs": len(chunked),
        "docs_with_no_chunks": sorted(d.path for d in chunked if not d.chunks),
        "sections": sum(d.sections for d in chunked),
        "empty_sections": sum(d.empty_sections for d in chunked),
        "single_small_chunk_docs": sorted(
            d.path for d in chunked if len(d.chunks) == 1 and count_tokens(d.chunks[0].body) < 40
        ),
    }
