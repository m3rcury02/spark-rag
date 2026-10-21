import json

import pytest

from docs_rag.chunk import ChunkConfig, chunk_document
from docs_rag.clean import clean_page
from docs_rag.evaluate import (
    ChunkSpan,
    EvalItem,
    Occurrence,
    bootstrap,
    first_relevant_rank,
    hit_at,
    item_violations,
    load_eval_set,
    longest_shared_run,
    occurrences,
    paired_difference,
    reciprocal_rank_at,
    relevant_ids,
    resamples,
)
from docs_rag.sections import split_sections

PAGE = (
    "---\ntitle: Tuning\n---\n# Memory\n\n"
    + " ".join(f"Sentence number {i} talks about memory settings in some detail." for i in range(30))
    + "\n\n# Shuffle\n\nThe shuffle service keeps files after executors exit, so dynamic allocation can "
    "remove them.\n"
)


def item(**over: object) -> EvalItem:
    base = dict(
        item_id="q01",
        question="Why can executors be removed while their shuffle output is kept?",
        answer="The shuffle service keeps the files.",
        path="docs/tuning.md",
        section_path=("Tuning", "Shuffle"),
        quote="The shuffle service keeps files after executors exit",
        kind="behavior",
        also_in=(),
    )
    return EvalItem(**{**base, **over})  # type: ignore[arg-type]


def texts_and_spans() -> tuple[dict[str, str], dict[tuple[str, ...], tuple[int, int]]]:
    page = clean_page(PAGE, "tuning")
    spans = {s.path: (s.start, s.start + len(s.body)) for s in split_sections(page.title, page.text)}
    return {"docs/tuning.md": page.text}, spans


def test_occurrences_finds_every_one_in_every_doc() -> None:
    found = occurrences("aba", {"b.md": "xabababa", "a.md": "aba"})
    assert [(o.path, o.start) for o in found] == [("a.md", 0), ("b.md", 1), ("b.md", 3), ("b.md", 5)]


def test_a_chunk_is_relevant_when_it_covers_half_the_quote() -> None:
    occ = [Occurrence("a.md", 100, 200)]
    chunks = [
        ChunkSpan("half", "a.md", 0, 150),  # covers 50 of 100
        ChunkSpan("less", "a.md", 151, 400),  # covers 49
        ChunkSpan("other_doc", "b.md", 0, 1000),
    ]
    assert relevant_ids(occ, chunks) == frozenset({"half"})


def test_quotes_across_overlapping_chunk_boundaries_still_have_a_relevant_chunk() -> None:
    texts, sections = texts_and_spans()
    doc = chunk_document("docs/tuning.md", PAGE, ChunkConfig(64, 8))
    spans = [ChunkSpan(c.chunk_id, c.path, c.char_start, c.char_end) for c in doc.chunks]
    start, end = sections[("Tuning", "Memory")]  # one long paragraph: its chunks overlap
    text = texts["docs/tuning.md"]
    assert len(spans) > 3
    for at in range(start, end - 80, 37):  # quotes everywhere in it, many straddling chunk boundaries
        quote = text[at : at + 80]
        assert relevant_ids(occurrences(quote, texts), spans), quote


def test_item_checks() -> None:
    texts, spans = texts_and_spans()
    assert item_violations(item(), texts, spans) == []
    assert "the quote does not occur in its section" in item_violations(
        item(section_path=("Tuning", "Memory")), texts, spans
    )
    assert any("outside" in p for p in item_violations(item(quote="too short"), texts, spans))
    assert any("no section" in p for p in item_violations(item(section_path=("x",)), texts, spans))


def test_longest_shared_run_measures_copying() -> None:
    assert longest_shared_run("Why are shuffle files kept?", "The shuffle service keeps files") == 1
    assert longest_shared_run("does the shuffle service keep files?", "The shuffle service keeps files") == 3


def test_ranks_and_metrics() -> None:
    rank = first_relevant_rank(["a", "b", "c"], frozenset({"c", "z"}))
    assert rank == 3
    assert (hit_at(rank, 5), hit_at(rank, 2), reciprocal_rank_at(rank, 10)) == (
        1.0,
        0.0,
        pytest.approx(1 / 3),
    )
    assert first_relevant_rank(["a"], frozenset({"z"})) is None
    assert (hit_at(None, 5), reciprocal_rank_at(None, 10)) == (0.0, 0.0)


def test_bootstrap_is_seeded_and_brackets_the_mean() -> None:
    values = [1.0] * 40 + [0.0] * 10
    draws = resamples(len(values), 2000, seed=1)
    assert draws == resamples(len(values), 2000, seed=1)
    est = bootstrap(values, draws)
    assert est.mean == 0.8 and est.low < 0.8 < est.high
    assert est.low > 0.65 and est.high < 0.93


def test_paired_difference_counts_items_each_side_won() -> None:
    a = [1.0, 1.0, 1.0, 0.0] * 10
    b = [1.0, 0.0, 0.0, 0.0] * 10
    d = paired_difference(a, b, resamples(40, 1000, seed=2))
    assert d.mean == 0.5 and (d.a_only, d.b_only) == (20, 0)
    assert d.low > 0 and d.share_above_zero == 1.0
    with pytest.raises(ValueError):
        paired_difference(a, b[:3], [[0]])


def test_load_eval_set_refuses_duplicate_ids() -> None:
    raw = {"version": "1", "tag": "v4.0.1", "commit_sha": "c", "cleaner_version": "2"}
    one = {
        "item_id": "q01",
        "question": "q",
        "answer": "a",
        "path": "p",
        "section_path": ["T"],
        "quote": "x" * 40,
        "kind": "fact",
    }
    assert load_eval_set(json.dumps({**raw, "items": [one]})).items[0].also_in == ()
    with pytest.raises(ValueError, match="not unique"):
        load_eval_set(json.dumps({**raw, "items": [one, one]}))
