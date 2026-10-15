import dataclasses
import datetime as dt

from docs_rag.bronze import (
    BRONZE_COLUMNS,
    BronzeRecord,
    StoredDoc,
    bronze_schema_ddl,
    check_against_stored,
    to_records,
)
from docs_rag.models import Snapshot, SourceDoc

AT = dt.datetime(2026, 10, 15, 12, 0, tzinfo=dt.UTC)


def snap(docs: dict[str, str], commit: str = "c1") -> Snapshot:
    return Snapshot(
        source_repo="apache/spark",
        version_tag="v4.0.1",
        commit_sha=commit,
        docs=tuple(
            SourceDoc(path=p, content="", content_sha256=h, size_bytes=0) for p, h in sorted(docs.items())
        ),
    )


def stored(docs: dict[str, str], commit: str = "c1") -> list[StoredDoc]:
    return [StoredDoc(path=p, content_sha256=h, commit_sha=commit) for p, h in docs.items()]


def test_record_fields_match_table_columns_in_order() -> None:
    """Rows are passed to Spark positionally, so a reordering here would silently swap columns."""
    assert [f.name for f in dataclasses.fields(BronzeRecord)] == [name for name, _ in BRONZE_COLUMNS]


def test_schema_ddl_lists_every_column_not_null() -> None:
    ddl = bronze_schema_ddl()
    assert ddl.startswith("source_repo STRING NOT NULL, version_tag STRING NOT NULL")
    assert ddl.count("NOT NULL") == len(BRONZE_COLUMNS)


def test_to_records_copies_every_doc_with_run_metadata() -> None:
    s = Snapshot("apache/spark", "v4.0.1", "c1", (SourceDoc("docs/a.md", "# A", "h", 3),))
    (rec,) = to_records(s, "run-1", AT)
    assert rec.as_row() == ("apache/spark", "v4.0.1", "c1", "docs/a.md", "# A", "h", 3, AT, "run-1")


def test_first_load_of_a_tag_is_new() -> None:
    check = check_against_stored(snap({"docs/a.md": "h1"}), [])
    assert (check.status, check.stored_docs, check.source_docs) == ("new", 0, 1)


def test_same_docs_and_commit_are_identical() -> None:
    docs = {"docs/a.md": "h1", "docs/b.md": "h2"}
    assert check_against_stored(snap(docs), stored(docs)).status == "identical"


def test_a_missing_doc_is_a_conflict_not_a_delete() -> None:
    check = check_against_stored(snap({"docs/a.md": "h1"}), stored({"docs/a.md": "h1", "docs/b.md": "h2"}))
    assert check.status == "conflict"
    assert check.only_stored == ("docs/b.md",)
    assert "only in bronze: docs/b.md" in check.describe()


def test_an_extra_doc_is_a_conflict() -> None:
    check = check_against_stored(snap({"docs/a.md": "h1", "docs/c.md": "h3"}), stored({"docs/a.md": "h1"}))
    assert (check.status, check.only_source) == ("conflict", ("docs/c.md",))


def test_changed_content_is_a_conflict() -> None:
    check = check_against_stored(snap({"docs/a.md": "new"}), stored({"docs/a.md": "old"}))
    assert (check.status, check.changed) == ("conflict", ("docs/a.md",))
    assert "content changed: docs/a.md" in check.describe()


def test_a_moved_tag_is_a_conflict_even_with_identical_docs() -> None:
    check = check_against_stored(snap({"docs/a.md": "h1"}, commit="c2"), stored({"docs/a.md": "h1"}, "c1"))
    assert check.status == "conflict"
    assert "commit in bronze ['c1'] vs source c2" in check.describe()


def test_describe_caps_long_path_lists() -> None:
    check = check_against_stored(snap({}), stored({f"docs/{i:02}.md": "h" for i in range(25)}))
    assert "(+15 more)" in check.describe()
