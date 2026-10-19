import dataclasses

import pytest

from docs_rag.changes import (
    COMMITTED,
    FAILED,
    RUN_LOG_COLUMNS,
    ChainError,
    ChunkRef,
    LogEntry,
    RunLogRow,
    chunk_violations,
    counts,
    diff_chunks,
    diff_docs,
    direction,
    docs_fingerprint,
    pick_baseline,
    row_violations,
)

BEFORE = {"docs/a.md": "h1", "docs/b.md": "h2", "docs/gone.md": "h3", "docs/same.md": "h4"}
AFTER = {"docs/a.md": "h1x", "docs/b.md": "h2", "docs/new.md": "h5", "docs/same.md": "h4"}


# --- docs ---


def test_docs_are_compared_by_path_and_hash() -> None:
    d = diff_docs(BEFORE, AFTER)
    assert (d.added, d.changed, d.deleted, d.unchanged, d.moved) == (
        ("docs/new.md",),
        ("docs/a.md",),
        ("docs/gone.md",),
        ("docs/b.md", "docs/same.md"),
        0,
    )


def test_the_initial_run_adds_everything_and_a_rerun_changes_nothing() -> None:
    assert diff_docs(None, AFTER).added == tuple(sorted(AFTER))
    again = diff_docs(AFTER, AFTER)
    assert (again.added, again.changed, again.deleted) == ((), (), ())


def test_a_rename_is_a_delete_plus_an_add_and_is_counted_as_moved() -> None:
    d = diff_docs({"docs/old.md": "h"}, {"docs/new.md": "h"})
    assert (d.added, d.deleted, d.changed, d.moved) == (("docs/new.md",), ("docs/old.md",), (), 1)


def test_the_diff_is_symmetric() -> None:
    there, back = diff_docs(BEFORE, AFTER), diff_docs(AFTER, BEFORE)
    assert (there.added, there.deleted, there.changed) == (back.deleted, back.added, back.changed)


def test_docs_fingerprint_ignores_order_and_sees_any_change() -> None:
    assert docs_fingerprint(dict(reversed(list(AFTER.items())))) == docs_fingerprint(AFTER)
    assert docs_fingerprint({**AFTER, "docs/a.md": "other"}) != docs_fingerprint(AFTER)


# --- chunks ---


def ref(path: str, cid: str, n: int, pos: int = 0) -> ChunkRef:
    return ChunkRef(path, cid, n, (pos, pos * 10, pos * 10 + 5))


DOCS = diff_docs(
    {"docs/a.md": "1", "docs/gone.md": "2", "docs/b.md": "3"},
    {"docs/a.md": "1x", "docs/new.md": "4", "docs/b.md": "3"},
)
BASE = [
    ref("docs/a.md", "a1", 10, 0),
    ref("docs/a.md", "a2", 20, 1),
    ref("docs/gone.md", "g1", 5),
    ref("docs/b.md", "b1", 7),
]
TARGET = [
    ref("docs/a.md", "a1", 10, 1),
    ref("docs/a.md", "a3", 30, 0),
    ref("docs/new.md", "n1", 4),
    ref("docs/b.md", "b1", 7),
]


def test_chunks_are_split_by_the_kind_of_doc_they_belong_to() -> None:
    c = diff_chunks(BASE, TARGET, DOCS)
    assert (c.added, c.removed, c.kept, c.tokens_to_embed) == (("a3", "n1"), ("a2", "g1"), 2, 34)
    assert c.added_by_doc == {"new": 1, "changed": 1, "unchanged": 0}
    assert c.removed_by_doc == {"changed": 1, "unchanged": 0, "deleted": 1}
    assert c.kept_relocated == 1  # a1 moved from position 0 to 1
    assert c.changed_docs_with_same_chunks == ()


def test_a_changed_doc_can_keep_all_its_chunks_but_a_doc_without_chunks_does_not_count() -> None:
    docs = diff_docs({"docs/a.md": "1", "docs/gen.md": "2"}, {"docs/a.md": "1x", "docs/gen.md": "2x"})
    same = [ref("docs/a.md", "a1", 10)]
    assert diff_chunks(same, same, docs).changed_docs_with_same_chunks == ("docs/a.md",)


def test_consistent_chunkings_pass() -> None:
    assert chunk_violations(BASE, TARGET, DOCS, frozenset(), same_config=True) == []


def test_an_unchanged_doc_with_other_chunks_is_drift_unless_the_config_changed() -> None:
    drifted = [*TARGET[:3], ref("docs/b.md", "b1-other", 7)]
    assert any("unchanged docs" in p for p in chunk_violations(BASE, drifted, DOCS, frozenset(), True))
    assert chunk_violations(BASE, drifted, DOCS, frozenset(), same_config=False) == []


def test_duplicates_stray_paths_and_unexpected_empty_docs_are_caught() -> None:
    bad = [*TARGET[1:], ref("docs/b.md", "b1", 7), ref("docs/ghost.md", "x", 1)]  # a.md keeps a3 only
    problems = chunk_violations(BASE, bad, DOCS, frozenset(), True)
    assert any("used more than once" in p for p in problems)
    assert any("not docs of the snapshot" in p for p in problems)
    no_new = [c for c in TARGET if c.path != "docs/new.md"]
    assert any("may not be empty" in p for p in chunk_violations(BASE, no_new, DOCS, frozenset(), True))
    assert chunk_violations(BASE, no_new, DOCS, frozenset({"docs/new.md"}), True) == []


# --- tags and the chain ---


@pytest.mark.parametrize(
    "a,b,expected",
    [
        (None, "v3.5.0", "initial"),
        ("v3.5.0", "v4.0.0", "forward"),
        ("v4.0.0", "v4.0.0", "same"),
        ("v4.0.1", "v4.0.0", "backward"),
        ("v3.5.0", "v3.10.0", "forward"),  # numeric, not string, order
        ("v4.0.0", "v4.0.0-rc1", "unordered"),
    ],
)
def test_direction_orders_release_numbers(a: str | None, b: str, expected: str) -> None:
    assert direction(a, b) == expected


def test_no_committed_run_means_no_baseline() -> None:
    assert pick_baseline([]) is None
    assert pick_baseline([LogEntry("r1", None, FAILED)]) is None


def test_the_baseline_is_the_head_of_the_committed_chain_and_failed_runs_are_ignored() -> None:
    log = [
        LogEntry("r1", None, COMMITTED),
        LogEntry("r2", "r1", COMMITTED),
        LogEntry("r3", "r2", FAILED),
        LogEntry("r4", "r2", COMMITTED),
    ]
    assert pick_baseline(log) == "r4"


@pytest.mark.parametrize(
    "log,message",
    [
        (
            [
                LogEntry("r1", None, COMMITTED),
                LogEntry("r2", "r1", COMMITTED),
                LogEntry("r3", "r1", COMMITTED),
            ],
            "forked",
        ),
        ([LogEntry("r1", None, COMMITTED), LogEntry("r2", None, COMMITTED)], "exactly one initial"),
        ([LogEntry("r1", None, COMMITTED), LogEntry("r2", "r9", COMMITTED)], "not committed"),
        ([LogEntry("r1", None, COMMITTED), LogEntry("r1", None, COMMITTED)], "more than once"),
    ],
)
def test_a_forked_or_broken_chain_raises(log: list[LogEntry], message: str) -> None:
    with pytest.raises(ChainError, match=message):
        pick_baseline(log)


# --- the run log row ---


def test_row_fields_match_table_columns_in_order() -> None:
    assert [f.name for f in dataclasses.fields(RunLogRow)] == [name for name, _ in RUN_LOG_COLUMNS]


def committed_row(**overrides: object) -> RunLogRow:
    base = RunLogRow(
        run_id="r2",
        baseline_run_id="r1",
        job="change_detect",
        source_repo="apache/spark",
        sink="none",
        status=COMMITTED,
        error=None,
        direction="forward",
        from_version_tag="v1",
        from_commit_sha="c1",
        from_config_id="cfg",
        to_version_tag="v2",
        to_commit_sha="c2",
        to_config_id="cfg",
        max_tokens=254,
        overlap_tokens=32,
        **counts(DOCS, diff_chunks(BASE, TARGET, DOCS), from_chunks=len(BASE), to_tokens=51),  # type: ignore[arg-type]
    )
    return dataclasses.replace(base, **overrides)  # type: ignore[arg-type]


def test_a_committed_row_built_from_the_diffs_adds_up() -> None:
    row = committed_row()
    assert row_violations(row) == []
    assert (row.from_docs, row.to_docs, row.from_chunks, row.to_chunks) == (3, 3, 4, 4)
    assert row.as_row()["added_paths"] == ["docs/new.md"]


def test_rows_that_do_not_add_up_are_refused() -> None:
    assert "to_chunks = kept + added" in row_violations(committed_row(to_chunks=99))
    assert "path lists match the counts" in row_violations(committed_row(added_paths=()))
    assert "a committed row has a missing count" in row_violations(committed_row(docs_added=None))
    assert "an initial run has no baseline" in row_violations(committed_row(baseline_run_id=None))
    unchanged_chunks = committed_row(chunks_added_unchanged_docs=1, chunks_added_new_docs=0)
    assert "a same-config run adds or removes no chunks of unchanged docs" in row_violations(unchanged_chunks)


def test_a_failed_row_needs_no_counts() -> None:
    assert row_violations(committed_row(status=FAILED, docs_added=None)) == []
    assert row_violations(committed_row(status="detected")) == ["unknown status 'detected'"]
