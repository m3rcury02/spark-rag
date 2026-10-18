"""Read the docs of a source tag straight from git, independently of the pipeline's own fetch code.

A blob-less, sparse checkout of docs/**/*.md fetches only the Markdown blobs; the files are then read as raw
blobs (`git cat-file`), so no checkout filter or line-ending setting can change their bytes. Decoded as
strict UTF-8, as bronze does. Used by the M2 evidence scripts.
"""

import subprocess
import tempfile
from dataclasses import dataclass

SOURCE = "https://github.com/apache/spark"


@dataclass(frozen=True)
class TagDocs:
    tag: str
    commit_sha: str
    files: dict[str, str]  # path -> content


def _git(cwd: str, *args: str, data: bytes | None = None) -> bytes:
    return subprocess.run(["git", "-C", cwd, *args], input=data, capture_output=True, check=True).stdout


def read_tags(tags: list[str], docs_dir: str = "docs", suffix: str = ".md") -> dict[str, TagDocs]:
    out = {}
    with tempfile.TemporaryDirectory() as d:
        _git(d, "init", "-q")
        _git(d, "remote", "add", "origin", SOURCE)
        _git(d, "sparse-checkout", "set", "--no-cone", f"/{docs_dir}/**/*{suffix}")
        for tag in tags:
            _git(
                d,
                "fetch",
                "-q",
                "--depth",
                "1",
                "--filter=blob:none",
                "origin",
                f"refs/tags/{tag}:refs/tags/{tag}",
            )
            _git(d, "checkout", "-q", "--detach", tag)  # fetches the sparse paths' blobs in one batch
            # -z: NUL-separated and unquoted, so a path with non-ASCII characters still passes the filter.
            listing = _git(d, "ls-tree", "-r", "-z", tag, "--", docs_dir).decode().split("\0")
            blobs = []
            for entry in filter(None, listing):
                meta, path = entry.split("\t", 1)
                _mode, kind, blob_id = meta.split()
                if kind == "blob" and path.endswith(suffix):
                    blobs.append((path, blob_id))
            batch = _git(d, "cat-file", "--batch", data="".join(f"{b}\n" for _, b in blobs).encode())
            files, pos = {}, 0
            for path, blob_id in blobs:
                header_end = batch.index(b"\n", pos)
                got_id, _kind, size = batch[pos:header_end].decode().split()
                assert got_id == blob_id, (got_id, blob_id)
                start = header_end + 1
                files[path] = batch[start : start + int(size)].decode("utf-8", errors="strict")
                pos = start + int(size) + 1  # the content is followed by a newline
            commit = _git(d, "rev-parse", f"{tag}^{{commit}}").decode().strip()
            out[tag] = TagDocs(tag=tag, commit_sha=commit, files=files)
    return out
