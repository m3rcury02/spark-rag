"""Records that cross layer boundaries. Frozen, so a step cannot mutate what another step produced."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceDoc:
    """One Markdown file from the source repo at one tag."""

    path: str  # repo-relative, e.g. "docs/streaming/index.md"
    content: str  # decoded as strict UTF-8
    content_sha256: str  # hex digest of the raw bytes
    size_bytes: int


@dataclass(frozen=True)
class Snapshot:
    """All docs of one tag, read from one source archive."""

    source_repo: str
    version_tag: str
    commit_sha: str  # from the archive's pax header (git archive writes the commit id there)
    docs: tuple[SourceDoc, ...]  # sorted by path
