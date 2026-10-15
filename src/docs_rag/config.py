"""Typed pipeline configuration: one object passed into every step, so nothing is hard-coded in functions."""

import re
from dataclasses import dataclass

from docs_rag.chunk import ChunkConfig

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_TAG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _check(pattern: re.Pattern[str], value: str, what: str) -> str:
    if not pattern.fullmatch(value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


def validate_tag(tag: str) -> str:
    """A git tag name we accept, e.g. 'v4.0.1'. Tags end up in URLs and SQL parameters."""
    return _check(_TAG, tag, "tag")


@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"
    schema: str = "docs_rag_dev"
    source_repo: str = "apache/spark"
    docs_dir: str = "docs"
    doc_suffix: str = ".md"
    chunk: ChunkConfig = ChunkConfig()
    # Docs that legitimately give no chunks: only headings plus generated includes (D-008). Any other doc
    # with no chunks fails the silver run, so a cleaning bug that empties pages cannot pass silently.
    zero_chunk_docs: frozenset[str] = frozenset({"docs/sql-ref-functions-builtin.md"})

    def __post_init__(self) -> None:
        _check(_IDENTIFIER, self.catalog, "catalog")
        _check(_IDENTIFIER, self.schema, "schema")
        _check(_REPO, self.source_repo, "source repo")
        _check(_IDENTIFIER, self.docs_dir, "docs dir")
        if not self.doc_suffix.startswith("."):
            raise ValueError(f"invalid doc suffix: {self.doc_suffix!r}")

    def table(self, name: str) -> str:
        """Fully qualified Unity Catalog name, e.g. 'workspace.docs_rag_dev.bronze_docs'."""
        return f"{self.catalog}.{self.schema}.{_check(_IDENTIFIER, name, 'table name')}"

    @property
    def bronze_table(self) -> str:
        return self.table("bronze_docs")

    @property
    def silver_table(self) -> str:
        return self.table("silver_chunks")

    def volume(self, name: str) -> str:
        """Path of a Unity Catalog Volume in this schema, e.g. '/Volumes/workspace/docs_rag_dev/artifacts'."""
        return f"/Volumes/{self.catalog}/{self.schema}/{_check(_IDENTIFIER, name, 'volume name')}"
