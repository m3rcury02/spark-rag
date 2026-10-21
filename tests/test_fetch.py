import gzip
import io
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from docs_rag.config import Config
from docs_rag.fetch import archive_url, latest_release, read_snapshot, release_tags
from docs_rag.hashing import sha256_hex

COMMIT = "29434ea766b0fc3c3bf6eaadb43a8f931133649e"


def make_tar(
    files: dict[str, bytes],
    *,
    top: str | None = "spark-4.0.1",
    commit: str | None = COMMIT,
    symlinks: dict[str, str] | None = None,
) -> bytes:
    """An uncompressed tar shaped like `git archive`: one top folder, commit id in the pax global header."""
    buf = io.BytesIO()
    pax = {"comment": commit} if commit else {}
    prefix = f"{top}/" if top else ""
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT, pax_headers=pax) as tar:
        if top:
            dir_info = tarfile.TarInfo(top)
            dir_info.type = tarfile.DIRTYPE
            tar.addfile(dir_info)
        for name, data in files.items():
            info = tarfile.TarInfo(prefix + name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        for name, target in (symlinks or {}).items():
            info = tarfile.TarInfo(prefix + name)
            info.type = tarfile.SYMTYPE
            info.linkname = target
            tar.addfile(info)
    return buf.getvalue()


def make_archive(files: dict[str, bytes], **kwargs: object) -> bytes:
    return gzip.compress(make_tar(files, **kwargs))  # type: ignore[arg-type]


def snapshot_paths(archive: bytes, config: Config | None = None) -> list[str]:
    return [d.path for d in read_snapshot(archive, config or Config(), "v4.0.1").docs]


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
    assert snapshot_paths(archive) == ["docs/index.md", "docs/streaming/getting-started.md"]


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


def test_symlinks_and_directories_are_skipped() -> None:
    archive = make_archive({"docs/a.md": b"a"}, symlinks={"docs/link.md": "a.md"})
    assert snapshot_paths(archive) == ["docs/a.md"]


def test_suffix_match_is_case_sensitive() -> None:
    assert snapshot_paths(make_archive({"docs/a.md": b"a", "docs/B.MD": b"b"})) == ["docs/a.md"]


def test_docs_dir_and_suffix_come_from_config() -> None:
    archive = make_archive({"docs/a.md": b"a", "site/b.txt": b"b"})
    assert snapshot_paths(archive, Config(docs_dir="site", doc_suffix=".txt")) == ["site/b.txt"]


# --- a snapshot is all or nothing: anything that could make it partial must fail ---


def test_missing_gzip_trailer_is_rejected() -> None:
    archive = make_archive({"docs/a.md": b"a"})
    with pytest.raises(ValueError, match="not a complete gzip stream"):
        read_snapshot(archive[:-8], Config(), "v4.0.1")


def test_tar_cut_short_is_rejected_even_inside_a_valid_gzip_stream() -> None:
    tar = make_tar({f"docs/{i}.md": b"x" * 600 for i in range(6)})
    cut = tar[: 512 * 9]  # ends on a block boundary, mid-archive, before the end-of-archive marker
    with pytest.raises(ValueError, match="end-of-archive marker"):
        read_snapshot(gzip.compress(cut), Config(), "v4.0.1")


def test_no_docs_found_is_an_error_not_an_empty_snapshot() -> None:
    with pytest.raises(ValueError, match="no .md files under docs/"):
        read_snapshot(make_archive({"README.md": b"x"}), Config(), "v4.0.1")


def test_archive_without_a_top_folder_is_rejected() -> None:
    with pytest.raises(ValueError, match="file at the archive root"):
        read_snapshot(make_archive({"docs/a.md": b"a", "x": b"y"}, top=None), Config(), "v4.0.1")


def test_two_top_folders_are_rejected() -> None:
    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w", format=tarfile.PAX_FORMAT, pax_headers={"comment": COMMIT}) as t:
        for name in ("a/docs/x.md", "b/docs/y.md"):
            info = tarfile.TarInfo(name)
            info.size = 1
            t.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="one top-level folder"):
        read_snapshot(gzip.compress(tar.getvalue()), Config(), "v4.0.1")


def test_missing_commit_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="no commit id"):
        read_snapshot(make_archive({"docs/a.md": b"a"}, commit=None), Config(), "v4.0.1")


def test_invalid_utf8_fails_loudly_with_the_path() -> None:
    with pytest.raises(ValueError, match="docs/bad.md is not valid UTF-8"):
        read_snapshot(make_archive({"docs/bad.md": b"\xff\xfe"}), Config(), "v4.0.1")


def test_parent_directory_components_are_rejected() -> None:
    with pytest.raises(ValueError, match="unsafe path"):
        read_snapshot(make_archive({"docs/../docs/a.md": b"a"}), Config(), "v4.0.1")


def test_duplicate_paths_are_rejected() -> None:
    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w", format=tarfile.PAX_FORMAT, pax_headers={"comment": COMMIT}) as t:
        for data in (b"one", b"two"):
            info = tarfile.TarInfo("spark-4.0.1/docs/a.md")
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    with pytest.raises(ValueError, match="duplicate path"):
        read_snapshot(gzip.compress(tar.getvalue()), Config(), "v4.0.1")


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
def test_reads_a_real_git_archive(tmp_path: Path) -> None:
    """Not circular: the archive comes from `git archive`, the same tool behind GitHub's codeload."""

    def git(*args: str) -> str:
        identity = {f"GIT_{role}_{field}": value for role in ("AUTHOR", "COMMITTER")
                    for field, value in (("NAME", "t"), ("EMAIL", "t@example.com"))}  # fmt: skip
        # HOME points at the temp dir so the developer's global git config (e.g. signing) is not used.
        env = {**identity, "HOME": str(tmp_path), "PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        return subprocess.run(
            ["git", *args], cwd=tmp_path, env=env, capture_output=True, check=True
        ).stdout.decode()

    long_name = "docs/" + "n" * 120 + ".md"  # longer than a ustar name field: needs a pax path header
    for name, text in {"docs/index.md": "# Index\n", "docs/streaming/a.md": "# A\n", long_name: "# Long\n",
                       "README.md": "# Readme\n"}.items():  # fmt: skip
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(text)
    (tmp_path / "docs/link.md").symlink_to("index.md")
    git("init", "-q")
    git("add", ".")
    git("commit", "-q", "-m", "fixture")
    git("tag", "v9.9.9")
    head = git("rev-parse", "HEAD").strip()
    archive = subprocess.run(
        ["git", "archive", "--format=tar.gz", "--prefix=spark-9.9.9/", "v9.9.9"],
        cwd=tmp_path, capture_output=True, check=True,
    ).stdout  # fmt: skip

    snap = read_snapshot(archive, Config(), "v9.9.9")
    assert snap.commit_sha == head
    assert [d.path for d in snap.docs] == sorted(["docs/index.md", "docs/streaming/a.md", long_name])


def test_archive_url_points_at_the_tag() -> None:
    assert (
        archive_url("apache/spark", "v4.0.1")
        == "https://codeload.github.com/apache/spark/tar.gz/refs/tags/v4.0.1"
    )


@pytest.mark.parametrize("tag", ["", "../main", "v1 ; drop", "-v1"])
def test_archive_url_rejects_bad_tags(tag: str) -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        archive_url("apache/spark", tag)


ADVERTISEMENT = (
    "001e# service=git-upload-pack\n0000"
    "0155" + "a" * 40 + " HEAD\x00multi_ack thin-pack side-band\n"
    "003f" + "b" * 40 + " refs/heads/master\n"
    "004b" + "c" * 40 + " refs/tags/v4.0.0-preview1\n"
    "0041" + "d" * 40 + " refs/tags/v4.0.1\n"
    "0044" + "e" * 40 + " refs/tags/v4.0.1^{}\n"
    "0042" + "f" * 40 + " refs/tags/v4.10.0\n"
    "0041" + "1" * 40 + " refs/tags/v4.2.0\n"
    "0045" + "2" * 40 + " refs/tags/v4.2.0-rc1\n"
    "0000"
)


def test_release_tags_reads_plain_releases_from_the_ref_advertisement() -> None:
    assert release_tags(ADVERTISEMENT) == ["v4.0.1", "v4.2.0", "v4.10.0"]  # by release number, not text
    assert latest_release(release_tags(ADVERTISEMENT)) == "v4.10.0"
    with pytest.raises(ValueError, match="no release tags"):
        latest_release([])
