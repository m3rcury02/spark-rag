"""Bronze layer contract: the bronze_docs columns, the records written to it, and the immutability check.

Bronze holds one immutable snapshot per (source_repo, version_tag): one row per doc path (D-006). Loading a
tag that is already stored must reproduce it exactly. Any difference (a doc added, removed or changed, or a
different commit behind the tag) means the fetch is broken or the tag moved, and the load stops instead of
rewriting history. Re-ingesting a changed tag is a deliberate, manual step.
"""

import dataclasses
import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

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
    content_sha256: str
    commit_sha: str


@dataclass(frozen=True)
class SnapshotCheck:
    status: Literal["new", "identical", "conflict"]
    stored_docs: int
    source_docs: int
    only_stored: tuple[str, ...] = ()  # paths bronze has but the fetched snapshot does not
    only_source: tuple[str, ...] = ()  # paths the fetched snapshot has but bronze does not
    changed: tuple[str, ...] = ()  # same path, different content hash
    stored_commits: tuple[str, ...] = ()  # distinct commit ids in bronze for this tag
    source_commit: str = ""

    def describe(self) -> str:
        """Human-readable reason for a conflict, with at most 10 paths per category."""

        def head(paths: tuple[str, ...]) -> str:
            return ", ".join(paths[:10]) + (f" (+{len(paths) - 10} more)" if len(paths) > 10 else "")

        parts = [f"bronze holds {self.stored_docs} docs for this tag, the source has {self.source_docs}"]
        if self.stored_commits != (self.source_commit,):
            parts.append(f"commit in bronze {list(self.stored_commits)} vs source {self.source_commit}")
        for label, paths in (("only in bronze", self.only_stored), ("only in source", self.only_source)):
            if paths:
                parts.append(f"{label}: {head(paths)}")
        if self.changed:
            parts.append(f"content changed: {head(self.changed)}")
        return "; ".join(parts)


def check_against_stored(snapshot: Snapshot, stored: Iterable[StoredDoc]) -> SnapshotCheck:
    """Compare a fetched snapshot with what bronze holds for the same (repo, tag)."""
    stored_by_path = {s.path: s for s in stored}
    source_by_path = {d.path: d for d in snapshot.docs}
    n_stored, n_source = len(stored_by_path), len(source_by_path)
    if not stored_by_path:
        return SnapshotCheck("new", n_stored, n_source, source_commit=snapshot.commit_sha)
    only_stored = tuple(sorted(stored_by_path.keys() - source_by_path.keys()))
    only_source = tuple(sorted(source_by_path.keys() - stored_by_path.keys()))
    changed = tuple(
        sorted(
            p
            for p in stored_by_path.keys() & source_by_path.keys()
            if stored_by_path[p].content_sha256 != source_by_path[p].content_sha256
        )
    )
    stored_commits = tuple(sorted({s.commit_sha for s in stored_by_path.values()}))
    identical = (
        not only_stored and not only_source and not changed and stored_commits == (snapshot.commit_sha,)
    )
    return SnapshotCheck(
        "identical" if identical else "conflict",
        n_stored,
        n_source,
        only_stored=only_stored,
        only_source=only_source,
        changed=changed,
        stored_commits=stored_commits,
        source_commit=snapshot.commit_sha,
    )
