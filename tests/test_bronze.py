import dataclasses
import datetime as dt

from docs_rag.bronze import (
    BRONZE_COLUMNS,
    BronzeRecord,
    StoredDoc,
    bronze_schema_ddl,
    check_against_stored,
    invariant_violations,
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
    """Intact stored rows: the content still hashes to the stored hash."""
    return [
        StoredDoc(path=p, content_sha256=h, commit_sha=commit, content_sha256_actual=h)
        for p, h in docs.items()
    ]


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


def test_stored_content_that_no_longer_matches_its_hash_is_a_conflict() -> None:
    """The case the hash column alone cannot show: content edited, hash column untouched."""
    rows = [StoredDoc("docs/a.md", "h1", "c1", content_sha256_actual="something-else")]
    check = check_against_stored(snap({"docs/a.md": "h1"}), rows)
    assert (check.status, check.corrupt, check.changed) == ("conflict", ("docs/a.md",), ())
    assert "stored content does not match its stored hash: docs/a.md" in check.describe()


def test_a_path_stored_twice_is_a_conflict_whatever_the_row_order() -> None:
    same = stored({"docs/a.md": "h1"})
    for rows in (same + same, same + stored({"docs/a.md": "h1"}, commit="c2")):
        for ordered in (rows, rows[::-1]):
            check = check_against_stored(snap({"docs/a.md": "h1"}), ordered)
            assert (check.status, check.duplicated, check.stored_docs) == ("conflict", ("docs/a.md",), 2)


def test_commits_are_taken_from_every_stored_row() -> None:
    rows = stored({"docs/a.md": "h1"}) + stored({"docs/b.md": "h2"}, commit="c2")
    check = check_against_stored(snap({"docs/a.md": "h1", "docs/b.md": "h2"}), rows)
    assert (check.status, check.stored_commits) == ("conflict", ("c1", "c2"))


HEALTHY = {"docs": 3, "distinct_paths": 3, "commits": 1, "hash_mismatches": 0, "size_mismatches": 0}


def test_healthy_tag_has_no_violations() -> None:
    assert invariant_violations("v1", HEALTHY, expected_docs=3) == []


def test_invariant_violations_are_each_reported() -> None:
    stats = {
        "docs": "4",
        "distinct_paths": "3",
        "commits": "2",
        "hash_mismatches": "1",
        "size_mismatches": "1",
    }
    problems = invariant_violations("v1", stats, expected_docs=3)
    assert problems == [
        "v1: 4 rows but 3 distinct paths",
        "v1: 2 distinct commits",
        "v1: 1 rows where sha2(content, 256) <> content_sha256",
        "v1: 1 rows where octet_length(content) <> size_bytes",
        "v1: 4 rows but the source has 3 docs",
    ]


def test_row_count_is_only_compared_when_the_source_size_is_known() -> None:
    assert invariant_violations("v1", HEALTHY) == []


def test_describe_caps_long_path_lists() -> None:
    check = check_against_stored(snap({}), stored({f"docs/{i:02}.md": "h" for i in range(25)}))
    assert "(+15 more)" in check.describe()
