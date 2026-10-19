"""Change detection between two snapshots of the docs: which docs were added, changed or deleted (by content
hash, D-002), and which chunks that adds and removes (by chunk_id, D-010).

A doc is keyed by its path, so a rename is a delete plus an add. A chunk is keyed by its content-addressed
chunk_id, so there is no "changed" chunk: an edited chunk is one removed ID plus one added ID. Pure Python, so
the job and the local verification compute the same thing.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class DocDiff:
    added: tuple[str, ...]
    changed: tuple[str, ...]
    deleted: tuple[str, ...]
    unchanged: tuple[str, ...]


def diff_docs(baseline: Mapping[str, str] | None, target: Mapping[str, str]) -> DocDiff:
    """Compare {path: content_sha256} maps. With no baseline (the first run), every target doc is added."""
    before = baseline or {}
    return DocDiff(
        added=tuple(sorted(target.keys() - before.keys())),
        changed=tuple(sorted(p for p in target.keys() & before.keys() if target[p] != before[p])),
        deleted=tuple(sorted(before.keys() - target.keys())),
        unchanged=tuple(sorted(p for p in target.keys() & before.keys() if target[p] == before[p])),
    )


@dataclass(frozen=True)
class ChunkRef:
    path: str
    chunk_id: str
    n_tokens: int


@dataclass(frozen=True)
class ChunkDiff:
    added: int  # chunk_ids in the target only: what M5 has to embed
    removed: int  # chunk_ids in the baseline only: what M5 has to delete
    kept: int
    tokens_to_embed: int  # sum of n_tokens over the added chunks
    changed_docs_with_same_chunks: tuple[
        str, ...
    ]  # edits that do not reach the embedded text (front matter...)


def diff_chunks(baseline: Sequence[ChunkRef], target: Sequence[ChunkRef], docs: DocDiff) -> ChunkDiff:
    before = {c.chunk_id for c in baseline}
    after = {c.chunk_id for c in target}

    by_path_before = _by_path(baseline)
    by_path_after = _by_path(target)
    return ChunkDiff(
        added=len(after - before),
        removed=len(before - after),
        kept=len(after & before),
        tokens_to_embed=sum(c.n_tokens for c in target if c.chunk_id not in before),
        changed_docs_with_same_chunks=tuple(
            p for p in docs.changed if by_path_before.get(p, set()) == by_path_after.get(p, set())
        ),
    )


def _by_path(refs: Sequence[ChunkRef]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for c in refs:
        out.setdefault(c.path, set()).add(c.chunk_id)
    return out


def chunk_violations(baseline: Sequence[ChunkRef], target: Sequence[ChunkRef], docs: DocDiff) -> list[str]:
    """The two chunkings must agree with the doc diff. The chunker is deterministic and both sides use the
    same config, so an unchanged doc must have exactly the same chunk IDs in both; anything else means silver
    drifted or was edited. Chunk IDs are unique within a chunking, and every chunk belongs to a doc of its
    snapshot."""
    problems = []
    for name, refs in (("baseline", baseline), ("target", target)):
        dupes = [i for i, n in Counter(c.chunk_id for c in refs).items() if n > 1]
        if dupes:
            problems.append(f"{name}: {len(dupes)} chunk_ids used more than once, e.g. {dupes[:3]}")
    before, after = _by_path(baseline), _by_path(target)
    drifted = [p for p in docs.unchanged if before.get(p, set()) != after.get(p, set())]
    if drifted:
        problems.append(f"{len(drifted)} unchanged docs whose chunk IDs differ, e.g. {drifted[:3]}")
    target_docs = set(docs.added) | set(docs.changed) | set(docs.unchanged)
    baseline_docs = set(docs.changed) | set(docs.deleted) | set(docs.unchanged)
    stray = sorted((after.keys() - target_docs) | (before.keys() - baseline_docs))
    if stray:
        problems.append(f"{len(stray)} chunked paths that are not docs of their snapshot, e.g. {stray[:3]}")
    return problems


@dataclass(frozen=True)
class DocState:
    """One doc as both layers see it: bronze's hash and commit, and what its silver rows recorded."""

    content_sha256: str
    commit_sha: str


def silver_bronze_violations(
    silver: Mapping[str, set[DocState]], bronze: Mapping[str, DocState], zero_chunk_docs: frozenset[str]
) -> list[str]:
    """Silver must be the chunking of exactly this bronze snapshot: every bronze doc has chunks (unless it
    is expected to have none), every chunked path is a bronze doc, and each doc's silver rows all carry the
    bronze row's content hash and commit."""
    problems = []
    missing = sorted(bronze.keys() - silver.keys() - zero_chunk_docs)
    if missing:
        problems.append(f"{len(missing)} bronze docs with no silver chunks, e.g. {missing[:3]}")
    extra = sorted(silver.keys() - bronze.keys())
    if extra:
        problems.append(f"{len(extra)} silver paths not in bronze, e.g. {extra[:3]}")
    stale = sorted(p for p in silver.keys() & bronze.keys() if silver[p] != {bronze[p]})
    if stale:
        problems.append(
            f"{len(stale)} docs whose silver rows do not match bronze's hash/commit, e.g. {stale[:3]}"
        )
    return problems
