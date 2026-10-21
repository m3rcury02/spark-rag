"""Fetch one tag of the source repo and read its docs out of the GitHub source archive.

Only regular files under `<top>/<docs_dir>/` whose name ends in the doc suffix are kept, at any depth (4.x
tags have nested folders such as docs/streaming/). A snapshot must be complete, because loading a tag treats
it as the whole tag: the archive is fully decompressed (gzip checks its CRC and length), the tar must end with
its end-of-archive marker, and finding no docs is an error. A streaming read (`r|gz`) would instead stop
silently at a truncation and return a partial snapshot.
"""

import gzip
import io
import re
import tarfile
import urllib.parse
import urllib.request
from pathlib import PurePosixPath

from docs_rag.changes import release_number
from docs_rag.config import Config, validate_tag
from docs_rag.hashing import sha256_hex
from docs_rag.models import Snapshot, SourceDoc

_BLOCK = 512


def archive_url(repo: str, tag: str) -> str:
    """codeload URL of the tag's source archive (one request for the whole snapshot)."""
    return f"https://codeload.github.com/{repo}/tar.gz/refs/tags/{urllib.parse.quote(validate_tag(tag))}"


def refs_url(repo: str) -> str:
    """git's smart-HTTP ref advertisement for the repo: every branch and tag, no API rate limit."""
    return f"https://github.com/{repo}.git/info/refs?service=git-upload-pack"


_RELEASE_REF = re.compile(r"refs/tags/(v\d+\.\d+\.\d+)(?=\^\{\}|\x00|\n|$)")


def release_tags(advertisement: str) -> list[str]:
    """The plain release tags (vX.Y.Z; no previews or release candidates) in a ref advertisement, oldest
    first by release number."""
    tags = set(_RELEASE_REF.findall(advertisement))
    return sorted(tags, key=lambda t: release_number(t) or (0, 0, 0))


def latest_release(tags: list[str]) -> str:
    """The newest plain release tag."""
    if not tags:
        raise ValueError("no release tags found")
    return validate_tag(max(tags, key=lambda t: release_number(t) or (0, 0, 0)))


def download(url: str, timeout_seconds: float = 300.0) -> bytes:  # pragma: no cover - network
    request = urllib.request.Request(url, headers={"User-Agent": "docs-rag-ingest"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        data: bytes = response.read()
    return data


def _split_top(member_name: str) -> tuple[str, str]:
    """'spark-4.0.1/docs/x.md' -> ('spark-4.0.1', 'docs/x.md'). Every member must sit under a top folder."""
    parts = PurePosixPath(member_name).parts
    if member_name.startswith("/") or ".." in parts:
        raise ValueError(f"unsafe path in archive: {member_name!r}")
    if not parts:
        raise ValueError(f"empty path in archive: {member_name!r}")
    return parts[0], "/".join(parts[1:])


def read_snapshot(archive_gz: bytes, config: Config, tag: str) -> Snapshot:
    """Read every doc of one tag from a gzipped tar (as produced by GitHub's codeload, i.e. `git archive`)."""
    try:
        tar_bytes = gzip.decompress(archive_gz)
    except (EOFError, OSError) as e:  # truncated stream, bad CRC or length, not gzip at all
        raise ValueError(f"archive is not a complete gzip stream: {e}") from e

    docs: dict[str, SourceDoc] = {}
    tops: set[str] = set()
    prefix = f"{config.docs_dir}/"
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tar:
        for member in tar:
            top, path = _split_top(member.name)
            tops.add(top)
            if not path:
                if not member.isdir():
                    raise ValueError(f"file at the archive root: {member.name!r}")
                continue
            if not path.startswith(prefix) or not path.endswith(config.doc_suffix) or not member.isfile():
                continue  # other files, directories, symlinks, hard links
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
        end_marker = tar_bytes[tar.offset : tar.offset + 2 * _BLOCK]
        commit_sha = tar.pax_headers.get("comment")

    if end_marker != b"\0" * (2 * _BLOCK):
        raise ValueError("archive ends without the tar end-of-archive marker (truncated?)")
    if len(tops) != 1:
        raise ValueError(f"expected one top-level folder, found {sorted(tops)}")
    if not docs:
        raise ValueError(f"no {config.doc_suffix} files under {config.docs_dir}/ in the archive")
    if not commit_sha:
        raise ValueError("archive has no commit id in its pax header")
    return Snapshot(
        source_repo=config.source_repo,
        version_tag=validate_tag(tag),
        commit_sha=commit_sha,
        docs=tuple(docs[p] for p in sorted(docs)),
    )
