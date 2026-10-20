import dataclasses
import datetime as dt
import hashlib

import pytest

from docs_rag.chunk import ChunkConfig
from docs_rag.config import Config, DqConfig
from docs_rag.dq import (
    CHECKS,
    DQ_RESULTS_COLUMNS,
    FAIL,
    NOT_APPLICABLE,
    OVERRIDDEN,
    PASS,
    DqFailed,
    DqResult,
    DqRunContext,
    GatePassed,
    Totals,
    evaluate,
    gate,
    result_rows,
    validate_override,
)
from docs_rag.silver import BronzeDoc, SilverRecord, chunk_docs, to_records

AT = dt.datetime(2026, 10, 20, 12, 0, tzinfo=dt.UTC)
TAG = "v4.0.1"
COMMIT = "a" * 40
CONFIG = Config()
CHUNKING = CONFIG.chunk
REASON = "v4.0.0 removed 32 docs on purpose (D-002)"


def chunk(i: int, **overrides: object) -> SilverRecord:
    """A chunk that passes every check: 6+ body tokens, unique text and id, full metadata."""
    body = f"Section {i} explains how the planner handles case number {i}."
    base = SilverRecord(
        config_id="cfg",
        source_repo="apache/spark",
        version_tag=TAG,
        commit_sha=COMMIT,
        path=f"docs/page{i % 7}.md",
        doc_content_sha256="0" * 64,
        doc_clean_sha256="0" * 64,
        chunk_id=hashlib.sha256(str(i).encode()).hexdigest()[:32],
        occurrence=0,
        chunk_index=i,
        section_path=("Page", f"Section {i}"),
        body=body,
        embed_text=f"Page > Section {i}\n\n{body}",
        n_tokens=20,
        char_start=0,
        char_end=len(body),
        cleaner_version="1",
        chunker_version="1",
        max_tokens=CHUNKING.max_tokens,
        overlap_tokens=CHUNKING.overlap_tokens,
        chunked_at=AT,
        chunk_run_id="run1",
    )
    return dataclasses.replace(base, **overrides)  # type: ignore[arg-type]


def corpus(n: int = 100) -> list[SilverRecord]:
    return [chunk(i) for i in range(n)]


def by_check(results: list[DqResult]) -> dict[str, DqResult]:
    return {r.check: r for r in results}


def run(
    records: list[SilverRecord], baseline: Totals | None = None, accept: str | None = None, docs: int = 7
) -> dict[str, DqResult]:
    return by_check(evaluate(records, docs, baseline, CONFIG, CHUNKING, TAG, accept))


def totals(records: list[SilverRecord], docs: int = 7) -> Totals:
    return Totals(docs, len(records), sum(r.n_tokens for r in records))


def test_a_clean_initial_snapshot_passes_and_drift_is_not_applicable() -> None:
    results = evaluate(corpus(), 7, None, CONFIG, CHUNKING, TAG)
    assert [r.check for r in results] == list(CHECKS)
    statuses = {r.check: r.status for r in results}
    assert {statuses[c] for c in CHECKS[:6]} == {PASS}
    assert {statuses[c] for c in CHECKS[6:]} == {NOT_APPLICABLE}
    passed = gate(results, None)
    assert passed.overridden == () and passed.override_reason is None


def test_chunking_real_pages_passes() -> None:
    pages = {
        "docs/a.md": "---\ntitle: A\n---\n# One\n\nThe first section has a sentence or two.\n",
        "docs/b.md": "---\ntitle: B\n---\n# Two\n\nThe second page says something else entirely.\n",
    }
    docs = [BronzeDoc(p, c, "s", "s", COMMIT) for p, c in pages.items()]
    config = ChunkConfig(64, 8)
    records = to_records(docs, chunk_docs(docs, config), "apache/spark", TAG, config, "r", AT)
    results = evaluate(records, 2, totals(records, 2), CONFIG, config, TAG)
    assert {r.status for r in results} == {PASS}


def test_an_empty_body_fails_the_gate() -> None:
    results = evaluate([*corpus(), chunk(100, body="  \n")], 7, None, CONFIG, CHUNKING, TAG)
    assert by_check(results)["empty_chunks"].status == FAIL
    with pytest.raises(DqFailed, match="empty_chunks fail: 1/101"):
        gate(results, None)


def test_a_body_the_tokenizer_drops_entirely_is_empty() -> None:
    result = run([*corpus(), chunk(100, body="\u200b\u200b")])["empty_chunks"]  # zero-width spaces: 0 tokens
    assert (result.status, result.numerator) == (FAIL, 1)


def test_tiny_chunks_fail_only_above_their_share() -> None:
    tiny = [chunk(i, body="java:") for i in range(3)]
    at_limit = run([*tiny[:2], *corpus(100)[2:]])["tiny_chunks"]  # 2 of 100 = 2%: allowed
    assert (at_limit.status, at_limit.numerator, at_limit.denominator) == (PASS, 2, 100)
    assert at_limit.examples[0].endswith("2 tokens 'java:'")
    over = run([*tiny, *corpus(100)[3:]])["tiny_chunks"]
    assert (over.status, over.numerator) == (FAIL, 3)


def test_oversized_chunks_are_recounted() -> None:
    claims_small = chunk(100, embed_text="word " * 300, n_tokens=10)  # the stored count is not trusted
    claims_large = chunk(101, n_tokens=CHUNKING.max_tokens + 1)
    result = run([*corpus(), claims_small, claims_large])["oversized_chunks"]
    assert (result.status, result.numerator) == (FAIL, 2)
    assert result.examples == ("docs/page2.md #100: 300 tokens", "docs/page3.md #101: 255 tokens")


def test_oversized_is_the_tighter_of_max_tokens_and_the_model_limit() -> None:
    record = chunk(100, n_tokens=200)
    assert run([record])["oversized_chunks"].status == PASS  # 200 <= 254
    small = by_check(evaluate([record], 1, None, CONFIG, ChunkConfig(128, 16), TAG))
    assert small["oversized_chunks"].status == FAIL  # 200 > max_tokens 128
    big_chunks = ChunkConfig(300, 32)  # a config the model would truncate (Config refuses it in its grid)
    near = by_check(evaluate([chunk(100, n_tokens=255)], 1, None, CONFIG, big_chunks, TAG))
    assert near["oversized_chunks"].status == FAIL  # 255 > 256 - [CLS] - [SEP]
    assert "over 254 tokens" in near["oversized_chunks"].rule


def test_a_repeated_chunk_id_fails() -> None:
    records = corpus()
    result = run([*records, dataclasses.replace(records[5], chunk_index=500)])["duplicate_chunk_ids"]
    assert (result.status, result.numerator) == (FAIL, 1)


def test_repeated_embed_text_fails_only_above_its_share() -> None:
    records = corpus(100)
    one_copy = run([*records[:99], chunk(99, embed_text=records[0].embed_text)])["duplicate_embed_text"]
    assert (one_copy.status, one_copy.numerator) == (PASS, 1)
    two_copies = [*records[:98], *(chunk(i, embed_text=records[0].embed_text) for i in (98, 99))]
    result = run(two_copies)["duplicate_embed_text"]
    assert (result.status, result.numerator) == (FAIL, 2)
    assert result.examples[0].startswith("3x in ['docs/page0.md', 'docs/page1.md'")


@pytest.mark.parametrize(
    "overrides,problem",
    [
        ({"path": "README.md"}, "path"),
        ({"path": "docs/a.txt"}, "path"),
        ({"section_path": ()}, "section path"),
        ({"section_path": (" ", "Usage")}, "section path"),
        ({"section_path": ("Page", "")}, "section path"),
        ({"version_tag": "v4.0.0"}, "version_tag"),
        ({"commit_sha": "abc"}, "commit_sha"),
    ],
)
def test_missing_metadata_fails(overrides: dict[str, object], problem: str) -> None:
    result = run([*corpus(), chunk(100, **overrides)])["missing_metadata"]
    assert (result.status, result.numerator) == (FAIL, 1)
    assert result.examples[0].endswith(f": {problem}")


def test_no_chunks_at_all_fails() -> None:
    results = run([])
    assert results["empty_chunks"].status == FAIL
    assert results["empty_chunks"].examples == ("no chunks to check",)


@pytest.mark.parametrize(
    "baseline_docs,status",
    [(100, PASS), (91, PASS), (90, FAIL), (111, PASS), (112, FAIL)],
)
def test_drift_fails_above_ten_percent_either_way(baseline_docs: int, status: str) -> None:
    records = corpus()
    result = run(records, Totals(baseline_docs, 100, 2000), docs=100)["drift_docs"]
    assert result.status == status
    assert result.denominator == baseline_docs and result.numerator == 100


def test_drift_at_exactly_the_threshold_passes() -> None:
    result = run(corpus(), Totals(100, 100, 2000), docs=110)["drift_docs"]
    assert (result.status, result.observed) == (PASS, pytest.approx(0.10))


def test_accepted_drift_is_overridden_with_its_reason() -> None:
    results = evaluate(corpus(), 87, Totals(100, 100, 2000), CONFIG, CHUNKING, TAG, REASON)
    drift = by_check(results)["drift_docs"]
    assert (drift.status, drift.override_reason) == (OVERRIDDEN, REASON)
    assert by_check(results)["drift_chunks"].override_reason is None  # only the checks it overrode
    passed = gate(results, REASON)
    assert (passed.overridden, passed.override_reason) == (("drift_docs",), REASON)


def test_a_loss_beyond_the_ceiling_cannot_be_overridden_but_a_gain_can() -> None:
    lost = by_check(evaluate(corpus(), 49, Totals(100, 100, 2000), CONFIG, CHUNKING, TAG, REASON))
    assert lost["drift_docs"].status == FAIL
    assert lost["drift_docs"].examples == ("a loss this large cannot be overridden",)
    with pytest.raises(DqFailed, match=r"drift_docs fail: 100 -> 49 \(-51.0%"):
        gate(list(lost.values()), REASON)
    gained = by_check(evaluate(corpus(), 300, Totals(100, 100, 2000), CONFIG, CHUNKING, TAG, REASON))
    assert gained["drift_docs"].status == OVERRIDDEN


def test_tokens_and_chunks_drift_too() -> None:
    results = run(corpus(), Totals(7, 50, 4000))
    assert (results["drift_chunks"].status, results["drift_tokens"].status) == (FAIL, FAIL)
    assert results["drift_tokens"].numerator == 2000


def test_an_override_with_nothing_to_override_is_refused() -> None:
    records = corpus()
    results = evaluate(records, 7, totals(records), CONFIG, CHUNKING, TAG, REASON)
    assert {r.status for r in results} == {PASS}
    with pytest.raises(DqFailed, match="nothing to accept"):
        gate(results, REASON)
    initial = evaluate(records, 7, None, CONFIG, CHUNKING, TAG, REASON)
    with pytest.raises(DqFailed, match="nothing to accept"):
        gate(initial, REASON)


def test_gate_refuses_results_that_do_not_fit_the_rules() -> None:
    results = evaluate(corpus(), 7, None, CONFIG, CHUNKING, TAG)
    with pytest.raises(DqFailed, match="not one per check"):
        gate(results[:-1], None)
    with pytest.raises(DqFailed, match="not one per check"):
        gate([*results, results[0]], None)
    na_count_check = [dataclasses.replace(results[0], status=NOT_APPLICABLE), *results[1:]]
    with pytest.raises(DqFailed, match="cannot have"):
        gate(na_count_check, None)
    reasonless = [*results[:6], dataclasses.replace(results[6], status=OVERRIDDEN), *results[7:]]
    with pytest.raises(DqFailed, match="cannot have"):
        gate(reasonless, None)
    other_reason = [
        *results[:6],
        dataclasses.replace(results[6], status=OVERRIDDEN, override_reason="another reason here x"),
        *results[7:],
    ]
    with pytest.raises(DqFailed, match="other than"):
        gate(other_reason, REASON)


def test_a_gate_pass_cannot_be_made_outside_the_gate() -> None:
    with pytest.raises(TypeError, match="issued only by"):
        GatePassed(CHECKS, (), None)


def test_override_reasons_are_checked_before_the_run() -> None:
    dq = DqConfig()
    assert validate_override(None, initial=False, dq=dq) is None
    assert validate_override(f"  {REASON}\n", initial=False, dq=dq) == REASON
    with pytest.raises(ValueError, match="no baseline"):
        validate_override(REASON, initial=True, dq=dq)
    for weak in ("ok", "expected drift", "a b c d", "major release upgrade"):
        with pytest.raises(ValueError, match="at least 20 characters and 4 words"):
            validate_override(weak, initial=False, dq=dq)


def test_result_rows_match_the_table_columns() -> None:
    context = DqRunContext("r", "a", "apache/spark", "none", "cfg", None, TAG, None, AT, "0.1")
    rows = result_rows(evaluate(corpus(), 7, None, CONFIG, CHUNKING, TAG), context)
    assert len(rows) == len(CHECKS)
    assert all(list(row) == [name for name, _ in DQ_RESULTS_COLUMNS] for row in rows)
    assert isinstance(rows[0]["examples"], list)


def test_config_refuses_chunks_the_model_would_truncate() -> None:
    with pytest.raises(ValueError, match="does not fit"):
        Config(chunk_grid=(ChunkConfig(255, 32),))
    with pytest.raises(ValueError, match="does not fit"):
        Config(chunk=ChunkConfig(300, 32), embedding_max_seq_length=256)
    assert Config(chunk=ChunkConfig(300, 32), embedding_max_seq_length=512).chunk.max_tokens == 300


@pytest.mark.parametrize(
    "overrides",
    [{"max_tiny_share": 1.5}, {"max_drift": -0.1}, {"max_override_loss": 0.05}, {"tiny_body_tokens": 0}],
)
def test_dq_thresholds_are_validated(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        DqConfig(**overrides)  # type: ignore[arg-type]
