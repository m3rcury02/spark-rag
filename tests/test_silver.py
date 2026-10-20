import dataclasses
import datetime as dt
import hashlib

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig, config_id_of
from docs_rag.clean import CLEANER_VERSION
from docs_rag.silver import (
    SILVER_COLUMNS,
    BronzeDoc,
    SilverRecord,
    StoredChunk,
    bronze_violations,
    check_against_stored,
    chunk_docs,
    conflict_reason,
    fingerprint,
    record_violations,
    stored_baseline_violations,
    stored_violations,
    to_records,
    token_stats,
)

AT = dt.datetime(2026, 10, 15, 12, 0, tzinfo=dt.UTC)
CONFIG = ChunkConfig(max_tokens=64, overlap_tokens=8)
PAGES = {
    "docs/a.md": "---\ntitle: A\n---\n# One\n\nFirst section.\n\n# Two\n\nSecond section.\n",
    "docs/gen.md": "# Functions\n{% include_api_gen generated.html %}\n",
}


def bronze(pages: dict[str, str] = PAGES, commit: str = "c1") -> list[BronzeDoc]:
    docs = []
    for path, content in pages.items():
        sha = hashlib.sha256(content.encode()).hexdigest()
        docs.append(BronzeDoc(path, content, sha, sha, commit))
    return docs


def records(docs: list[BronzeDoc] | None = None) -> list[SilverRecord]:
    docs = bronze() if docs is None else docs
    return to_records(docs, chunk_docs(docs, CONFIG), "apache/spark", "v4.0.1", CONFIG, "run1", AT)


def stored(recs: list[SilverRecord]) -> list[StoredChunk]:
    names = {f.name for f in dataclasses.fields(StoredChunk)}
    return [StoredChunk(**{k: v for k, v in dataclasses.asdict(r).items() if k in names}) for r in recs]


def test_record_fields_match_table_columns_in_order() -> None:
    """Rows are passed to Spark positionally, so a reordering here would silently swap columns."""
    assert [f.name for f in dataclasses.fields(SilverRecord)] == [name for name, _ in SILVER_COLUMNS]


def test_rows_carry_section_path_as_a_list() -> None:
    row = records()[0].as_row()
    assert row[[n for n, _ in SILVER_COLUMNS].index("section_path")] == ["A", "One"]


# --- bronze input ---


def test_whole_bronze_snapshot_passes() -> None:
    assert bronze_violations(bronze()) == []


def test_bronze_problems_are_reported() -> None:
    docs = bronze()
    tampered = dataclasses.replace(docs[0], content_sha256_actual="0" * 64)
    other_commit = dataclasses.replace(docs[1], commit_sha="c2")
    problems = bronze_violations([tampered, docs[0], other_commit])
    assert len(problems) == 3  # two commits, a path stored twice, a hash mismatch
    assert bronze_violations([]) == ["no bronze rows for the tag"]


# --- a fresh chunking ---


def test_records_point_back_at_their_bronze_doc() -> None:
    recs = records()
    assert {(r.path, r.commit_sha) for r in recs} == {("docs/a.md", "c1")}
    assert all(r.doc_content_sha256 == bronze()[0].content_sha256 for r in recs)
    assert [r.section_path for r in recs] == [("A", "One"), ("A", "Two")]


def test_only_expected_docs_may_have_no_chunks() -> None:
    docs = bronze()
    chunked = chunk_docs(docs, CONFIG)
    recs = records(docs)
    assert record_violations(recs, chunked, CONFIG, frozenset({"docs/gen.md"})) == []
    problems = record_violations(recs, chunked, CONFIG, frozenset())
    assert problems == ["1 docs with no chunks that are not expected to be empty: ['docs/gen.md']"]


def test_residue_fails_the_chunking() -> None:
    docs = bronze()
    chunked = [dataclasses.replace(d, residue={"html_tags": 1}) for d in chunk_docs(docs, CONFIG)]
    problems = record_violations(records(docs), chunked, CONFIG, frozenset({"docs/gen.md"}))
    assert len(problems) == 1 and "markup left after cleaning" in problems[0]


# --- stored rows ---


def test_intact_stored_rows_pass() -> None:
    assert stored_violations(stored(records()), CONFIG) == []


def test_tampered_stored_rows_are_caught() -> None:
    rows = stored(records())
    bad = [
        dataclasses.replace(rows[0], embed_text=rows[0].embed_text + "!"),  # id and breadcrumb+body broken
        dataclasses.replace(rows[1], n_tokens=rows[1].n_tokens + 1),
    ]
    problems = stored_violations(bad, CONFIG)
    assert any("chunk_id does not match" in p for p in problems)
    assert any("not breadcrumb + body" in p for p in problems)
    assert any("n_tokens" in p for p in problems)


def test_rows_from_another_config_are_caught() -> None:
    problems = stored_violations(stored(records()), ChunkConfig(max_tokens=64, overlap_tokens=0))
    assert problems and all("another cleaner/chunker version or config" in p for p in problems)


def test_offsets_that_cannot_fit_the_body_are_caught() -> None:
    rows = stored(records())
    bad = [
        dataclasses.replace(rows[0], char_end=rows[0].char_start),
        dataclasses.replace(rows[1], char_start=-1),
    ]
    assert len([p for p in stored_violations(bad, CONFIG) if "offsets" in p]) == 2


def test_snapshot_comparison() -> None:
    recs = records()
    assert check_against_stored(recs, []).status == "new"
    assert check_against_stored(recs, stored(recs)).status == "identical"
    changed = records(bronze({**PAGES, "docs/a.md": PAGES["docs/a.md"].replace("First", "1st")}))
    check = check_against_stored(changed, stored(recs))
    # One section's text changed, but the doc hash is part of every row's identity, so both rows differ.
    assert (check.status, check.only_stored, check.only_computed) == ("conflict", 2, 2)
    moved = records(bronze(commit="c2"))
    assert check_against_stored(moved, stored(recs)).status == "conflict"  # same text, another commit


def test_a_conflict_names_moved_bronze_before_a_missing_bump() -> None:
    recs = stored(records())
    zero = frozenset({"docs/gen.md"})
    assert "bump CLEANER_VERSION" in conflict_reason(bronze(), recs, zero)  # bronze as stored: the code moved
    for moved in (
        bronze(commit="c2"),
        bronze({**PAGES, "docs/a.md": PAGES["docs/a.md"].replace("First", "1st")}),
        bronze({**PAGES, "docs/new.md": "# New\n\nText.\n"}),
        bronze({"docs/gen.md": PAGES["docs/gen.md"]}),  # docs/a.md removed
    ):
        reason = conflict_reason(moved, recs, zero)
        assert reason.startswith("bronze changed") and "docs/gen.md" not in reason, reason


# --- determinism ---


def test_fingerprint_ignores_order() -> None:
    pairs = [("docs/a.md", "x"), ("docs/b.md", "y")]
    assert fingerprint(pairs) == fingerprint(reversed(pairs)) != fingerprint(pairs[:1])


def test_token_stats() -> None:
    docs = bronze()
    stats = token_stats(records(docs), chunk_docs(docs, CONFIG))
    assert stats["chunks"] == 2 and stats["docs_with_no_chunks"] == ["docs/gen.md"]
    assert sum(stats["n_tokens_histogram"].values()) == 2  # type: ignore[attr-defined]


# --- the re-baseline path (D-012): a baseline checked from its stored rows ---

OLD = ("0", CHUNKER_VERSION)  # an earlier cleaner version than today's
OLD_ID = config_id_of(*OLD, CONFIG.max_tokens, CONFIG.overlap_tokens)
ZERO = frozenset({"docs/gen.md"})


def old_rows(recs: list[SilverRecord] | None = None) -> list[StoredChunk]:
    """Rows as an earlier cleaner version would have stored them (same text here; only the label differs)."""
    return [dataclasses.replace(r, cleaner_version=OLD[0]) for r in stored(recs or records())]


def test_config_id_of_matches_the_config_and_tells_versions_apart() -> None:
    assert config_id_of(CLEANER_VERSION, CHUNKER_VERSION, 64, 8) == CONFIG.config_id
    assert CONFIG.config_id != OLD_ID


def test_a_stored_baseline_of_an_earlier_version_passes() -> None:
    assert stored_baseline_violations(old_rows(), bronze(), OLD_ID, ZERO) == []


def test_a_stored_baseline_is_checked_row_by_row() -> None:
    rows = old_rows()
    tampered = [dataclasses.replace(rows[0], embed_text=rows[0].embed_text + " x"), *rows[1:]]
    assert any(
        "chunk_id does not match" in p for p in stored_baseline_violations(tampered, bronze(), OLD_ID, ZERO)
    )
    assert stored_baseline_violations([], bronze(), OLD_ID, ZERO) == ["no stored rows"]
    mixed = [dataclasses.replace(rows[0], cleaner_version="9"), *rows[1:]]
    assert (
        "rows of 2 cleaner/chunker versions" in stored_baseline_violations(mixed, bronze(), OLD_ID, ZERO)[0]
    )
    wrong_id = stored_baseline_violations(rows, bronze(), CONFIG.config_id, ZERO)
    assert any("do not hash to config_id" in p for p in wrong_id)


def test_a_stored_baseline_must_belong_to_the_bronze_snapshot() -> None:
    rows = old_rows()
    assert any(
        "another commit" in p for p in stored_baseline_violations(rows, bronze(commit="c2"), OLD_ID, ZERO)
    )
    other_doc = bronze({**PAGES, "docs/new.md": "# New\n\nA page silver never saw.\n"})
    assert any("no stored rows" in p for p in stored_baseline_violations(rows, other_doc, OLD_ID, ZERO))
    fewer = bronze({"docs/gen.md": PAGES["docs/gen.md"]})
    assert any("not in bronze" in p for p in stored_baseline_violations(rows, fewer, OLD_ID, ZERO))
