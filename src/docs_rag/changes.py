"""Change detection between the last committed snapshot and a new tag, and the run log recording it (D-011).

Docs are compared by path and content hash (D-002), so a rename is a delete plus an add. Chunks are compared
by their content-addressed chunk_id (D-010), so there is no "changed" chunk: an edit is one removed ID plus
one added ID. Each run appends one row to ops_run_log. A row with status 'committed' is a baseline: the next
run diffs against the head of the chain of committed rows for the same (source_repo, sink), where each row
names the run it diffed against. No timestamps decide anything. Pure Python, so the job and the tests share
it.
"""

import datetime as dt
import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from typing import Literal

COMMITTED = "committed"  # the only status that makes a baseline; written only by a run's final step
FAILED = "failed"


# --- docs ---------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DocDiff:
    added: tuple[str, ...]
    changed: tuple[str, ...]
    deleted: tuple[str, ...]
    unchanged: tuple[str, ...]
    moved: int  # added docs whose content equals a deleted doc's: a rename or move (still delete + add)


def diff_docs(baseline: Mapping[str, str] | None, target: Mapping[str, str]) -> DocDiff:
    """Compare {path: content_sha256} maps. With no baseline (the initial run), every target doc is added."""
    before = baseline or {}
    added = tuple(sorted(target.keys() - before.keys()))
    deleted = tuple(sorted(before.keys() - target.keys()))
    deleted_hashes = {before[p] for p in deleted}
    return DocDiff(
        added=added,
        changed=tuple(sorted(p for p in target.keys() & before.keys() if target[p] != before[p])),
        deleted=deleted,
        unchanged=tuple(sorted(p for p in target.keys() & before.keys() if target[p] == before[p])),
        moved=sum(target[p] in deleted_hashes for p in added),
    )


def docs_fingerprint(docs: Mapping[str, str]) -> str:
    """sha256 of the sorted path/hash lines: one value that pins a snapshot's docs."""
    return hashlib.sha256("".join(sorted(f"{p}\t{h}\n" for p, h in docs.items())).encode()).hexdigest()


# --- chunks -------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ChunkRef:
    path: str
    chunk_id: str
    n_tokens: int
    position: tuple[int, int, int]  # (chunk_index, char_start, char_end): where it sits, not what it is


@dataclass(frozen=True)
class ChunkDiff:
    added: tuple[str, ...]  # chunk_ids only in the target: what to embed
    removed: tuple[str, ...]  # chunk_ids only in the baseline: what to delete
    kept: int
    kept_relocated: int  # kept IDs whose position changed: positions must come from silver, not gold
    tokens_to_embed: int
    added_by_doc: dict[str, int]  # 'new' / 'changed' / 'unchanged' doc -> added chunks
    removed_by_doc: dict[str, int]  # 'changed' / 'unchanged' / 'deleted' doc -> removed chunks
    changed_docs_with_same_chunks: tuple[str, ...]  # edits that never reach embedded text (doc has chunks)


def diff_chunks(baseline: Sequence[ChunkRef], target: Sequence[ChunkRef], docs: DocDiff) -> ChunkDiff:
    before = {c.chunk_id: c for c in baseline}
    after = {c.chunk_id: c for c in target}
    kind = {p: "new" for p in docs.added} | {p: "changed" for p in docs.changed}
    kind |= {p: "unchanged" for p in docs.unchanged} | {p: "deleted" for p in docs.deleted}
    added = sorted(after.keys() - before.keys())
    removed = sorted(before.keys() - after.keys())
    kept = after.keys() & before.keys()
    ids_before, ids_after = _ids_by_path(baseline), _ids_by_path(target)
    return ChunkDiff(
        added=tuple(added),
        removed=tuple(removed),
        kept=len(kept),
        kept_relocated=sum(after[i].position != before[i].position for i in kept),
        tokens_to_embed=sum(after[i].n_tokens for i in added),
        added_by_doc={
            k: sum(kind[after[i].path] == k for i in added) for k in ("new", "changed", "unchanged")
        },
        removed_by_doc={
            k: sum(kind[before[i].path] == k for i in removed) for k in ("changed", "unchanged", "deleted")
        },
        changed_docs_with_same_chunks=tuple(
            p for p in docs.changed if ids_before.get(p) and ids_before.get(p) == ids_after.get(p)
        ),
    )


def _ids_by_path(refs: Iterable[ChunkRef]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for c in refs:
        out.setdefault(c.path, set()).add(c.chunk_id)
    return out


def chunk_violations(
    baseline: Sequence[ChunkRef],
    target: Sequence[ChunkRef],
    docs: DocDiff,
    zero_chunk_docs: frozenset[str],
    same_config: bool,
) -> list[str]:
    """The chunkings must fit the doc diff: chunk IDs unique within each side; every chunked path a doc of its
    snapshot; a doc without chunks only if it may have none (D-008); and, when both sides use the same chunk
    config, an unchanged doc has exactly the same chunk IDs on both sides (the chunker is deterministic)."""
    problems = []
    before, after = _ids_by_path(baseline), _ids_by_path(target)
    target_docs = set(docs.added) | set(docs.changed) | set(docs.unchanged)
    baseline_docs = set(docs.changed) | set(docs.deleted) | set(docs.unchanged)
    for name, refs, ids, snapshot in (
        ("baseline", baseline, before, baseline_docs),
        ("target", target, after, target_docs),
    ):
        if len({c.chunk_id for c in refs}) != len(refs):
            problems.append(f"{name}: a chunk_id is used more than once")
        stray = sorted(ids.keys() - snapshot)
        if stray:
            problems.append(
                f"{name}: {len(stray)} chunked paths are not docs of the snapshot, e.g. {stray[:3]}"
            )
        empty = sorted(snapshot - ids.keys() - zero_chunk_docs)
        if empty:
            problems.append(
                f"{name}: {len(empty)} docs without chunks that may not be empty, e.g. {empty[:3]}"
            )
    if same_config:
        drifted = [p for p in docs.unchanged if before.get(p, set()) != after.get(p, set())]
        if drifted:
            problems.append(f"{len(drifted)} unchanged docs whose chunk IDs differ, e.g. {drifted[:3]}")
    return problems


# --- tags and the baseline chain ---------------------------------------------------------------------------

_VERSION = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")
Direction = Literal["initial", "forward", "same", "backward", "unordered"]


def release_number(tag: str) -> tuple[int, int, int] | None:
    """(major, minor, patch) of a plain release tag (vX.Y.Z), else None."""
    m = _VERSION.fullmatch(tag)
    if not m:
        return None
    major, minor, patch = map(int, m.groups())
    return major, minor, patch


def direction(from_tag: str | None, to_tag: str) -> Direction:
    """How the target relates to the baseline tag, by release number (vX.Y.Z); tags that are not plain
    release numbers cannot be ordered."""
    if from_tag is None:
        return "initial"
    if from_tag == to_tag:
        return "same"
    va, vb = release_number(from_tag), release_number(to_tag)
    if va is None or vb is None:
        return "unordered"
    return "forward" if vb > va else "backward" if vb < va else "unordered"


ReleaseKind = Literal["patch", "minor", "major"]


def release_kind(from_tag: str, to_tag: str) -> ReleaseKind:
    """How far apart two tags are, whichever is older: 'major' or 'minor' when that number differs, else
    'patch'. The same tag, and any tag that is not a plain release number, count as 'patch', the strictest."""
    va, vb = release_number(from_tag), release_number(to_tag)
    if va is None or vb is None:
        return "patch"
    return "major" if va[0] != vb[0] else "minor" if va[1] != vb[1] else "patch"


class ChainError(RuntimeError):
    """The run log does not give exactly one baseline: forked, broken, or emptied."""


@dataclass(frozen=True)
class LogEntry:
    """What baseline selection needs from one ops_run_log row."""

    run_id: str
    baseline_run_id: str | None
    status: str


def pick_baseline(entries: Sequence[LogEntry]) -> str | None:
    """The run_id of the head of the committed chain (the committed run that no other committed run diffed
    against), or None when nothing is committed. Raises when the chain is forked or broken."""
    committed = [e for e in entries if e.status == COMMITTED]
    if not committed:
        return None
    ids = {e.run_id for e in committed}
    if len(ids) != len(committed):
        raise ChainError("a run_id is logged as committed more than once")
    referenced = [e.baseline_run_id for e in committed if e.baseline_run_id is not None]
    if len(referenced) != len(set(referenced)):
        raise ChainError("two committed runs diffed against the same baseline: the chain forked")
    if sum(e.baseline_run_id is None for e in committed) != 1:
        raise ChainError("the committed chain must have exactly one initial run")
    dangling = sorted(set(referenced) - ids)
    if dangling:
        raise ChainError(f"committed runs name baselines that are not committed: {dangling[:3]}")
    heads = sorted(ids - set(referenced))
    if len(heads) != 1:
        raise ChainError(f"expected one chain head, found {len(heads)}")
    return heads[0]


# --- the run log row --------------------------------------------------------------------------------------

RUN_LOG_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING NOT NULL"),
    ("baseline_run_id", "STRING"),  # NULL only for the initial run of a chain (and failed runs without one)
    ("job", "STRING NOT NULL"),
    ("source_repo", "STRING NOT NULL"),
    ("sink", "STRING NOT NULL"),  # what a committed run published to; 'none' until M5 adds gold
    ("status", "STRING NOT NULL"),  # 'committed' | 'failed'
    ("error", "STRING"),
    ("direction", "STRING"),
    ("from_version_tag", "STRING"),
    ("from_commit_sha", "STRING"),
    ("from_config_id", "STRING"),
    ("to_version_tag", "STRING NOT NULL"),
    ("to_commit_sha", "STRING"),
    ("to_config_id", "STRING NOT NULL"),
    ("max_tokens", "INT NOT NULL"),
    ("overlap_tokens", "INT NOT NULL"),
    ("from_docs", "INT"),
    ("to_docs", "INT"),
    ("docs_added", "INT"),
    ("docs_changed", "INT"),
    ("docs_deleted", "INT"),
    ("docs_unchanged", "INT"),
    ("docs_moved", "INT"),
    ("docs_changed_same_chunks", "INT"),
    ("from_chunks", "INT"),
    ("to_chunks", "INT"),
    ("chunks_added_new_docs", "INT"),
    ("chunks_added_changed_docs", "INT"),
    ("chunks_added_unchanged_docs", "INT"),  # 0 unless the chunk config changed
    ("chunks_removed_changed_docs", "INT"),
    ("chunks_removed_unchanged_docs", "INT"),  # 0 unless the chunk config changed
    ("chunks_removed_deleted_docs", "INT"),
    ("chunks_kept", "INT"),
    ("chunks_kept_relocated", "INT"),
    ("tokens_to_embed", "BIGINT"),
    ("to_tokens", "BIGINT"),
    ("added_paths", "ARRAY<STRING>"),
    ("changed_paths", "ARRAY<STRING>"),
    ("deleted_paths", "ARRAY<STRING>"),
    ("to_docs_fingerprint", "STRING"),  # docs_fingerprint of the target bronze: pins the snapshot
    ("to_chunks_fingerprint", "STRING"),  # silver.fingerprint of the target (path, chunk_id) rows
    ("started_at", "TIMESTAMP NOT NULL"),
    ("finished_at", "TIMESTAMP NOT NULL"),
    ("code_version", "STRING NOT NULL"),
    # Added in M4 (D-012) with ALTER TABLE ADD COLUMNS, so it comes last: the --accept-drift reason of a run
    # committed with an overridden drift check; NULL otherwise. Its checks are in ops_dq_results.
    ("dq_override", "STRING"),
    # Added after M4 the same way: the failed run whose logged drift the override accepted (D-012).
    ("dq_override_of", "STRING"),
    # Added the same way: how the baseline side was checked, 'rechunked' (re-chunked from bronze and compared
    # with silver, D-011) or 'stored' (its config cannot be re-chunked by this code: its stored silver rows
    # against the logged fingerprint, D-013's re-baseline path). NULL for an initial run and early failures.
    ("baseline_check", "STRING"),
)


def run_log_ddl() -> str:
    return ", ".join(f"{name} {kind}" for name, kind in RUN_LOG_COLUMNS)


@dataclass(frozen=True)
class RunLogRow:
    run_id: str
    baseline_run_id: str | None
    job: str
    source_repo: str
    sink: str
    status: str
    error: str | None
    direction: str | None
    from_version_tag: str | None
    from_commit_sha: str | None
    from_config_id: str | None
    to_version_tag: str
    to_commit_sha: str | None
    to_config_id: str
    max_tokens: int
    overlap_tokens: int
    from_docs: int | None = None
    to_docs: int | None = None
    docs_added: int | None = None
    docs_changed: int | None = None
    docs_deleted: int | None = None
    docs_unchanged: int | None = None
    docs_moved: int | None = None
    docs_changed_same_chunks: int | None = None
    from_chunks: int | None = None
    to_chunks: int | None = None
    chunks_added_new_docs: int | None = None
    chunks_added_changed_docs: int | None = None
    chunks_added_unchanged_docs: int | None = None
    chunks_removed_changed_docs: int | None = None
    chunks_removed_unchanged_docs: int | None = None
    chunks_removed_deleted_docs: int | None = None
    chunks_kept: int | None = None
    chunks_kept_relocated: int | None = None
    tokens_to_embed: int | None = None
    to_tokens: int | None = None
    added_paths: tuple[str, ...] | None = None
    changed_paths: tuple[str, ...] | None = None
    deleted_paths: tuple[str, ...] | None = None
    to_docs_fingerprint: str | None = None
    to_chunks_fingerprint: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    code_version: str = ""
    dq_override: str | None = None
    dq_override_of: str | None = None
    baseline_check: str | None = None

    def as_row(self) -> dict[str, object]:
        row: dict[str, object] = {f.name: getattr(self, f.name) for f in fields(self)}
        for name in ("added_paths", "changed_paths", "deleted_paths"):
            value = row[name]
            row[name] = list(value) if isinstance(value, tuple) else value
        return row


def counts(docs: DocDiff, chunks: ChunkDiff, from_chunks: int, to_tokens: int) -> dict[str, object]:
    """The count columns of a committed row."""
    return {
        "from_docs": len(docs.changed) + len(docs.deleted) + len(docs.unchanged),
        "to_docs": len(docs.added) + len(docs.changed) + len(docs.unchanged),
        "docs_added": len(docs.added),
        "docs_changed": len(docs.changed),
        "docs_deleted": len(docs.deleted),
        "docs_unchanged": len(docs.unchanged),
        "docs_moved": docs.moved,
        "docs_changed_same_chunks": len(chunks.changed_docs_with_same_chunks),
        "from_chunks": from_chunks,
        "to_chunks": from_chunks + len(chunks.added) - len(chunks.removed),
        "chunks_added_new_docs": chunks.added_by_doc["new"],
        "chunks_added_changed_docs": chunks.added_by_doc["changed"],
        "chunks_added_unchanged_docs": chunks.added_by_doc["unchanged"],
        "chunks_removed_changed_docs": chunks.removed_by_doc["changed"],
        "chunks_removed_unchanged_docs": chunks.removed_by_doc["unchanged"],
        "chunks_removed_deleted_docs": chunks.removed_by_doc["deleted"],
        "chunks_kept": chunks.kept,
        "chunks_kept_relocated": chunks.kept_relocated,
        "tokens_to_embed": chunks.tokens_to_embed,
        "to_tokens": to_tokens,
        "added_paths": docs.added,
        "changed_paths": docs.changed,
        "deleted_paths": docs.deleted,
    }


_COUNT_COLUMNS = tuple(
    name for name, kind in RUN_LOG_COLUMNS if kind in ("INT", "BIGINT") or name.endswith("_paths")
)


def row_violations(row: RunLogRow) -> list[str]:
    """What a row must satisfy before it is written; a committed row's counts must add up."""
    if row.started_at is None or row.finished_at is None or not row.code_version:
        return ["started_at, finished_at and code_version are required"]
    if row.status == FAILED:
        overridden = row.dq_override is not None or row.dq_override_of is not None
        return ["only a committed row carries a DQ override"] if overridden else []
    if row.status != COMMITTED:
        return [f"unknown status {row.status!r}"]
    if any(getattr(row, name) is None for name in _COUNT_COLUMNS):
        return ["a committed row has a missing count"]

    def n(name: str) -> int:
        value = getattr(row, name)
        return int(value)

    added = n("chunks_added_new_docs") + n("chunks_added_changed_docs") + n("chunks_added_unchanged_docs")
    removed = (
        n("chunks_removed_changed_docs")
        + n("chunks_removed_unchanged_docs")
        + n("chunks_removed_deleted_docs")
    )
    checks = {
        "to_docs = added + changed + unchanged": n("to_docs")
        == n("docs_added") + n("docs_changed") + n("docs_unchanged"),
        "from_docs = changed + deleted + unchanged": n("from_docs")
        == n("docs_changed") + n("docs_deleted") + n("docs_unchanged"),
        "to_chunks = kept + added": n("to_chunks") == n("chunks_kept") + added,
        "from_chunks = kept + removed": n("from_chunks") == n("chunks_kept") + removed,
        "path lists match the counts": len(row.added_paths or ()) == n("docs_added")
        and len(row.changed_paths or ()) == n("docs_changed")
        and len(row.deleted_paths or ()) == n("docs_deleted"),
        "a same-config run adds or removes no chunks of unchanged docs": row.from_config_id
        != row.to_config_id
        or row.from_config_id is None
        or (n("chunks_added_unchanged_docs") == 0 and n("chunks_removed_unchanged_docs") == 0),
        "an initial run has no baseline": (row.direction == "initial") == (row.baseline_run_id is None),
        "a DQ override names its reason and the failed run it accepts": (row.dq_override is None)
        == (row.dq_override_of is None),
        "a run with a baseline says how it checked it": (row.baseline_check in ("rechunked", "stored"))
        == (row.baseline_run_id is not None),
    }
    return [name for name, ok in checks.items() if not ok]
