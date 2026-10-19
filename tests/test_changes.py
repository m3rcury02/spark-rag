from docs_rag.changes import (
    ChunkRef,
    DocState,
    chunk_violations,
    diff_chunks,
    diff_docs,
    silver_bronze_violations,
)

BEFORE = {"docs/a.md": "h1", "docs/b.md": "h2", "docs/gone.md": "h3", "docs/same.md": "h4"}
AFTER = {"docs/a.md": "h1x", "docs/b.md": "h2", "docs/new.md": "h5", "docs/same.md": "h4"}


def test_docs_are_compared_by_path_and_hash() -> None:
    d = diff_docs(BEFORE, AFTER)
    assert (d.added, d.changed, d.deleted, d.unchanged) == (
        ("docs/new.md",),
        ("docs/a.md",),
        ("docs/gone.md",),
        ("docs/b.md", "docs/same.md"),
    )


def test_the_first_run_adds_everything_and_a_rerun_changes_nothing() -> None:
    assert diff_docs(None, AFTER).added == tuple(sorted(AFTER))
    again = diff_docs(AFTER, AFTER)
    assert (again.added, again.changed, again.deleted) == ((), (), ())


def test_a_rename_is_a_delete_plus_an_add() -> None:
    d = diff_docs({"docs/old.md": "h"}, {"docs/new.md": "h"})
    assert (d.added, d.deleted, d.changed) == (("docs/new.md",), ("docs/old.md",), ())


def refs(*rows: tuple[str, str, int]) -> list[ChunkRef]:
    return [ChunkRef(*r) for r in rows]


BASE = refs(
    ("docs/a.md", "a1", 10), ("docs/a.md", "a2", 20), ("docs/gone.md", "g1", 5), ("docs/b.md", "b1", 7)
)
TARGET = refs(
    ("docs/a.md", "a1", 10), ("docs/a.md", "a3", 30), ("docs/new.md", "n1", 4), ("docs/b.md", "b1", 7)
)
DOCS = diff_docs(
    {"docs/a.md": "1", "docs/gone.md": "2", "docs/b.md": "3"},
    {"docs/a.md": "1x", "docs/new.md": "4", "docs/b.md": "3"},
)


def test_chunks_are_added_removed_or_kept_and_only_added_ones_cost_tokens() -> None:
    c = diff_chunks(BASE, TARGET, DOCS)
    assert (c.added, c.removed, c.kept, c.tokens_to_embed) == (2, 2, 2, 34)
    assert c.changed_docs_with_same_chunks == ()


def test_a_changed_doc_can_keep_all_its_chunks() -> None:
    docs = diff_docs({"docs/a.md": "1"}, {"docs/a.md": "front matter edited"})
    same = refs(("docs/a.md", "a1", 10))
    c = diff_chunks(same, same, docs)
    assert (c.added, c.removed, c.changed_docs_with_same_chunks) == (0, 0, ("docs/a.md",))


def test_consistent_chunkings_pass() -> None:
    assert chunk_violations(BASE, TARGET, DOCS) == []


def test_an_unchanged_doc_with_different_chunks_is_drift() -> None:
    drifted = [*TARGET[:3], ChunkRef("docs/b.md", "b1-edited", 7)]
    assert any("unchanged docs whose chunk IDs differ" in p for p in chunk_violations(BASE, drifted, DOCS))


def test_duplicate_ids_and_stray_paths_are_caught() -> None:
    bad = [*TARGET, ChunkRef("docs/a.md", "a1", 10), ChunkRef("docs/ghost.md", "x", 1)]
    problems = chunk_violations(BASE, bad, DOCS)
    assert any("used more than once" in p for p in problems)
    assert any("not docs of their snapshot" in p for p in problems)


def test_silver_must_be_the_chunking_of_this_bronze() -> None:
    bronze = {
        "docs/a.md": DocState("h1", "c1"),
        "docs/gen.md": DocState("h2", "c1"),
        "docs/b.md": DocState("h3", "c1"),
    }
    silver = {"docs/a.md": {DocState("h1", "c1")}, "docs/b.md": {DocState("h3", "c1")}}
    assert silver_bronze_violations(silver, bronze, frozenset({"docs/gen.md"})) == []
    stale = {**silver, "docs/b.md": {DocState("h3-old", "c0")}, "docs/x.md": {DocState("h", "c1")}}
    problems = silver_bronze_violations(stale, bronze, frozenset())
    assert len(problems) == 3  # gen.md has no chunks, x.md is not in bronze, b.md is stale
