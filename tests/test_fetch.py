import io
import tarfile

import pytest

from docs_rag.config import Config
from docs_rag.fetch import archive_url, read_snapshot
from docs_rag.hashing import sha256_hex

COMMIT = "29434ea766b0fc3c3bf6eaadb43a8f931133649e"


def make_archive(
    files: dict[str, bytes],
    *,
    top: str = "spark-4.0.1",
    commit: str | None = COMMIT,
    symlinks: dict[str, str] | None = None,
) -> io.BytesIO:
    """A gzipped tar shaped like a GitHub codeload archive: one top folder, commit sha in the pax header."""
    buf = io.BytesIO()
    pax = {"comment": commit} if commit else {}
    with tarfile.open(fileobj=buf, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers=pax) as tar:
        dir_info = tarfile.TarInfo(f"{top}/docs")
        dir_info.type = tarfile.DIRTYPE
        tar.addfile(dir_info)
        for name, data in files.items():
            info = tarfile.TarInfo(f"{top}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        for name, target in (symlinks or {}).items():
            info = tarfile.TarInfo(f"{top}/{name}")
            info.type = tarfile.SYMTYPE
            info.linkname = target
            tar.addfile(info)
    buf.seek(0)
    return buf


def test_keeps_only_markdown_under_docs_at_any_depth() -> None:
    archive = make_archive(
        {
            "docs/index.md": b"# Index",
            "docs/streaming/getting-started.md": b"# Streaming",
            "docs/img/logo.png": b"\x89PNG",
            "docs/_config.yml": b"x: 1",
            "README.md": b"# Spark",
            "python/docs/index.md": b"# not the docs folder",
        }
    )
    snap = read_snapshot(archive, Config(), "v4.0.1")
    assert [d.path for d in snap.docs] == ["docs/index.md", "docs/streaming/getting-started.md"]


def test_docs_are_sorted_and_hashed_over_raw_bytes() -> None:
    raw = "# Café\n".encode()
    snap = read_snapshot(make_archive({"docs/z.md": b"z", "docs/a.md": raw}), Config(), "v4.0.1")
    assert [d.path for d in snap.docs] == ["docs/a.md", "docs/z.md"]
    doc = snap.docs[0]
    assert doc.content == "# Café\n"
    assert doc.content_sha256 == sha256_hex(raw)
    assert doc.size_bytes == len(raw) == 8


def test_snapshot_carries_repo_tag_and_commit_from_pax_header() -> None:
    snap = read_snapshot(make_archive({"docs/a.md": b"a"}), Config(), "v4.0.1")
    assert (snap.source_repo, snap.version_tag, snap.commit_sha) == ("apache/spark", "v4.0.1", COMMIT)


def test_commit_is_none_when_archive_has_no_pax_comment() -> None:
    snap = read_snapshot(make_archive({"docs/a.md": b"a"}, commit=None), Config(), "v4.0.1")
    assert snap.commit_sha is None


def test_symlinks_and_directories_are_skipped() -> None:
    archive = make_archive({"docs/a.md": b"a"}, symlinks={"docs/link.md": "a.md"})
    snap = read_snapshot(archive, Config(), "v4.0.1")
    assert [d.path for d in snap.docs] == ["docs/a.md"]


def test_suffix_match_is_case_sensitive() -> None:
    snap = read_snapshot(make_archive({"docs/a.md": b"a", "docs/B.MD": b"b"}), Config(), "v4.0.1")
    assert [d.path for d in snap.docs] == ["docs/a.md"]


def test_invalid_utf8_fails_loudly_with_the_path() -> None:
    with pytest.raises(ValueError, match="docs/bad.md is not valid UTF-8"):
        read_snapshot(make_archive({"docs/bad.md": b"\xff\xfe"}), Config(), "v4.0.1")


def test_parent_directory_components_are_rejected() -> None:
    with pytest.raises(ValueError, match="unsafe path"):
        read_snapshot(make_archive({"docs/../docs/a.md": b"a"}), Config(), "v4.0.1")


def test_duplicate_paths_are_rejected() -> None:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", format=tarfile.PAX_FORMAT) as tar:
        for data in (b"one", b"two"):
            info = tarfile.TarInfo("spark-4.0.1/docs/a.md")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    buf.seek(0)
    with pytest.raises(ValueError, match="duplicate path"):
        read_snapshot(buf, Config(), "v4.0.1")


def test_empty_archive_gives_an_empty_snapshot() -> None:
    snap = read_snapshot(make_archive({}), Config(), "v4.0.1")
    assert snap.docs == ()


def test_docs_dir_and_suffix_come_from_config() -> None:
    archive = make_archive({"docs/a.md": b"a", "site/b.txt": b"b"})
    snap = read_snapshot(archive, Config(docs_dir="site", doc_suffix=".txt"), "v4.0.1")
    assert [d.path for d in snap.docs] == ["site/b.txt"]


def test_archive_url_points_at_the_tag() -> None:
    assert (
        archive_url("apache/spark", "v4.0.1")
        == "https://codeload.github.com/apache/spark/tar.gz/refs/tags/v4.0.1"
    )


@pytest.mark.parametrize("tag", ["", "../main", "v1 ; drop", "-v1"])
def test_archive_url_rejects_bad_tags(tag: str) -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        archive_url("apache/spark", tag)
