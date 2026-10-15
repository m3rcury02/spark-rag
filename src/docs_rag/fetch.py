"""Fetch one tag of the source repo and read its docs out of the GitHub source archive.

The archive is read as a stream and never extracted to disk. Only regular files under `<top>/<docs_dir>/`
whose name ends in the doc suffix are kept, at any depth (4.x tags have nested folders such as
docs/streaming/).
"""

import tarfile
import urllib.parse
import urllib.request
from pathlib import PurePosixPath
from typing import IO

from docs_rag.config import Config, validate_tag
from docs_rag.hashing import sha256_hex
from docs_rag.models import Snapshot, SourceDoc


def archive_url(repo: str, tag: str) -> str:
    """codeload URL of the tag's source archive (one request for the whole snapshot)."""
    return f"https://codeload.github.com/{repo}/tar.gz/refs/tags/{urllib.parse.quote(validate_tag(tag))}"


def download(url: str, timeout_seconds: float = 300.0) -> bytes:  # pragma: no cover - network
    request = urllib.request.Request(url, headers={"User-Agent": "docs-rag-ingest"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        data: bytes = response.read()
    return data


def _repo_relative(member_name: str) -> str | None:
    """Strip the archive's top-level folder ('spark-4.0.1/docs/x.md' -> 'docs/x.md')."""
    parts = PurePosixPath(member_name).parts
    if any(p in ("..", "") for p in parts) or member_name.startswith("/"):
        raise ValueError(f"unsafe path in archive: {member_name!r}")
    return "/".join(parts[1:]) if len(parts) > 1 else None


def read_snapshot(archive: IO[bytes], config: Config, tag: str) -> Snapshot:
    """Read every doc of one tag from a gzipped tar stream (as produced by GitHub's codeload)."""
    docs: dict[str, SourceDoc] = {}
    prefix = f"{config.docs_dir}/"
    with tarfile.open(fileobj=archive, mode="r|gz") as tar:
        for member in tar:
            path = _repo_relative(member.name)
            if path is None or not path.startswith(prefix) or not path.endswith(config.doc_suffix):
                continue
            if not member.isfile():  # directories, symlinks, hard links
                continue
            handle = tar.extractfile(member)
            if handle is None:  # pragma: no cover - isfile() members always have data
                continue
            raw = handle.read()
            try:
                content = raw.decode("utf-8")
            except UnicodeDecodeError as e:
                raise ValueError(f"{path} is not valid UTF-8: {e}") from e
            if path in docs:
                raise ValueError(f"duplicate path in archive: {path}")
            docs[path] = SourceDoc(
                path=path, content=content, content_sha256=sha256_hex(raw), size_bytes=len(raw)
            )
        commit_sha = tar.pax_headers.get("comment")
    return Snapshot(
        source_repo=config.source_repo,
        version_tag=validate_tag(tag),
        commit_sha=commit_sha,
        docs=tuple(docs[p] for p in sorted(docs)),
    )
