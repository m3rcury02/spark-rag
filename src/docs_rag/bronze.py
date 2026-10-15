"""Bronze layer contract: the bronze_docs columns, the records written to it, and the immutability check.

Bronze holds one immutable snapshot per (source_repo, version_tag): one row per doc path (D-006). Loading a
tag that is already stored must reproduce it exactly. Any difference (a doc added, removed or changed, a
different commit behind the tag, a stored row whose content no longer matches its hash, or a path stored
twice) means the fetch is broken, the tag moved or bronze was altered, and the load stops instead of
rewriting history. Re-ingesting a changed tag is a deliberate, manual step, and only for unpublished tags.
"""

import dataclasses
import datetime as dt
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from docs_rag.models import Snapshot

# Column order here is the table's column order and the field order of BronzeRecord (a test enforces both).
BRONZE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_repo", "STRING NOT NULL"),
    ("version_tag", "STRING NOT NULL"),
    ("commit_sha", "STRING NOT NULL"),
    ("path", "STRING NOT NULL"),
    ("content", "STRING NOT NULL"),
    ("content_sha256", "STRING NOT NULL"),
    ("size_bytes", "INT NOT NULL"),
    ("ingested_at", "TIMESTAMP NOT NULL"),
    ("ingest_run_id", "STRING NOT NULL"),
)


def bronze_schema_ddl() -> str:
    """'source_repo STRING NOT NULL, ...' for CREATE TABLE and createDataFrame."""
    return ", ".join(f"{name} {sql_type}" for name, sql_type in BRONZE_COLUMNS)


@dataclass(frozen=True)
class BronzeRecord:
    source_repo: str
    version_tag: str
    commit_sha: str
    path: str
    content: str
    content_sha256: str
    size_bytes: int
    ingested_at: dt.datetime
    ingest_run_id: str

    def as_row(self) -> tuple[object, ...]:
        return dataclasses.astuple(self)


def to_records(snapshot: Snapshot, run_id: str, ingested_at: dt.datetime) -> list[BronzeRecord]:
    return [
        BronzeRecord(
            source_repo=snapshot.source_repo,
            version_tag=snapshot.version_tag,
            commit_sha=snapshot.commit_sha,
            path=doc.path,
            content=doc.content,
            content_sha256=doc.content_sha256,
            size_bytes=doc.size_bytes,
            ingested_at=ingested_at,
            ingest_run_id=run_id,
        )
        for doc in snapshot.docs
    ]


@dataclass(frozen=True)
class StoredDoc:
    """What bronze already holds for one path of the tag being loaded."""

    path: str
    content_sha256: str  # the hash column as stored
    commit_sha: str
    content_sha256_actual: str  # sha2(content, 256) recomputed from the stored content when it was read


@dataclass(frozen=True)
class SnapshotCheck:
    status: Literal["new", "identical", "conflict"]
    stored_docs: int  # stored rows, duplicates included
    source_docs: int
    only_stored: tuple[str, ...] = ()  # paths bronze has but the fetched snapshot does not
    only_source: tuple[str, ...] = ()  # paths the fetched snapshot has but bronze does not
    changed: tuple[str, ...] = ()  # same path, different content hash
    corrupt: tuple[str, ...] = ()  # stored content no longer matches its own stored hash
    duplicated: tuple[str, ...] = ()  # paths stored more than once
    stored_commits: tuple[str, ...] = ()  # distinct commit ids across all stored rows
    source_commit: str = ""

    def describe(self) -> str:
        """Human-readable reason for a conflict, with at most 10 paths per category."""

        def head(paths: tuple[str, ...]) -> str:
            return ", ".join(paths[:10]) + (f" (+{len(paths) - 10} more)" if len(paths) > 10 else "")

        parts = [f"bronze holds {self.stored_docs} rows for this tag, the source has {self.source_docs} docs"]
        if self.stored_commits and self.stored_commits != (self.source_commit,):
            parts.append(f"commit in bronze {list(self.stored_commits)} vs source {self.source_commit}")
        for label, paths in (
            ("only in bronze", self.only_stored),
            ("only in source", self.only_source),
            ("content changed", self.changed),
            ("stored content does not match its stored hash", self.corrupt),
            ("stored more than once", self.duplicated),
        ):
            if paths:
                parts.append(f"{label}: {head(paths)}")
        return "; ".join(parts)


def check_against_stored(snapshot: Snapshot, stored: Iterable[StoredDoc]) -> SnapshotCheck:
    """Compare a fetched snapshot with what bronze holds for the same (repo, tag).

    "identical" requires every stored row to be intact (its content still hashes to its stored hash), each
    path to be stored once, and paths, hashes and the commit to equal the snapshot's.
    """
    rows = list(stored)
    source_by_path = {d.path: d for d in snapshot.docs}
    if not rows:
        return SnapshotCheck("new", 0, len(source_by_path), source_commit=snapshot.commit_sha)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.path] = counts.get(r.path, 0) + 1
    stored_paths = counts.keys()
    duplicated = tuple(sorted(p for p, n in counts.items() if n > 1))
    corrupt = tuple(sorted({r.path for r in rows if r.content_sha256 != r.content_sha256_actual}))
    only_stored = tuple(sorted(stored_paths - source_by_path.keys()))
    only_source = tuple(sorted(source_by_path.keys() - stored_paths))
    changed = tuple(
        sorted(
            {
                r.path
                for r in rows
                if r.path in source_by_path and r.content_sha256 != source_by_path[r.path].content_sha256
            }
        )
    )
    stored_commits = tuple(sorted({r.commit_sha for r in rows}))
    identical = not (only_stored or only_source or changed or corrupt or duplicated) and stored_commits == (
        snapshot.commit_sha,
    )
    return SnapshotCheck(
        "identical" if identical else "conflict",
        len(rows),
        len(source_by_path),
        only_stored=only_stored,
        only_source=only_source,
        changed=changed,
        corrupt=corrupt,
        duplicated=duplicated,
        stored_commits=stored_commits,
        source_commit=snapshot.commit_sha,
    )


def invariant_violations(tag: str, stats: Mapping[str, Any], expected_docs: int | None = None) -> list[str]:
    """Problems in one tag's bronze rows, from the job's per-tag statistics query. Empty means healthy.

    `stats` has docs, distinct_paths, commits, hash_mismatches, size_mismatches. `expected_docs` is the
    fetched snapshot's size, when this tag was just loaded.
    """
    n = {
        k: int(stats[k]) for k in ("docs", "distinct_paths", "commits", "hash_mismatches", "size_mismatches")
    }
    problems = []
    if n["docs"] != n["distinct_paths"]:
        problems.append(f"{tag}: {n['docs']} rows but {n['distinct_paths']} distinct paths")
    if n["commits"] != 1:
        problems.append(f"{tag}: {n['commits']} distinct commits")
    if n["hash_mismatches"]:
        problems.append(f"{tag}: {n['hash_mismatches']} rows where sha2(content, 256) <> content_sha256")
    if n["size_mismatches"]:
        problems.append(f"{tag}: {n['size_mismatches']} rows where octet_length(content) <> size_bytes")
    if expected_docs is not None and n["docs"] != expected_docs:
        problems.append(f"{tag}: {n['docs']} rows but the source has {expected_docs} docs")
    return problems
