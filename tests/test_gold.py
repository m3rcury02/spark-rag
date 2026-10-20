import dataclasses
import datetime as dt
import math

import pytest

from docs_rag.chunk import ChunkConfig
from docs_rag.config import EmbeddingConfig
from docs_rag.embedder import Canary, canary_cosines, cosine
from docs_rag.gold import (
    EMPTY,
    GOLD_COLUMNS,
    GoldRow,
    check_gold_state,
    check_merge_counts,
    gold_fingerprint,
    gold_rows,
)
from docs_rag.silver import BronzeDoc, chunk_docs, to_records

AT = dt.datetime(2026, 10, 20, 12, 0, tzinfo=dt.UTC)
PAGES = {
    "docs/a.md": "---\ntitle: A\n---\n# One\n\nFirst section text here.\n\n# Two\n\nSecond section text.\n"
}


def records():  # type: ignore[no-untyped-def]
    docs = [BronzeDoc(p, c, "s", "s", "c" * 40) for p, c in PAGES.items()]
    config = ChunkConfig(64, 8)
    return to_records(docs, chunk_docs(docs, config), "apache/spark", "v4.0.1", config, "r", AT)


def test_row_fields_match_table_columns_in_order() -> None:
    assert [f.name for f in dataclasses.fields(GoldRow)] == [name for name, _ in GOLD_COLUMNS]


def test_gold_rows_carry_only_chunk_determined_columns_and_the_run() -> None:
    recs = records()
    rows = gold_rows(recs, [[0.6, 0.8]] * len(recs), "m@rev", "run9", AT)
    assert [r.chunk_id for r in rows] == [r.chunk_id for r in recs]
    first = rows[0].as_row()
    assert first["breadcrumb"] == " > ".join(recs[0].section_path)
    assert first["embedding"] == [0.6, 0.8] and isinstance(first["section_path"], list)
    assert (first["embedding_model"], first["embedded_by_run_id"]) == ("m@rev", "run9")
    assert not {"version_tag", "chunk_index", "char_start", "config_id"} & set(first)
    with pytest.raises(ValueError, match="records but"):
        gold_rows(recs, [[1.0]], "m", "r", AT)


def test_gold_must_hold_the_chain_head_before_a_run_changes_it() -> None:
    base, target = gold_fingerprint([("p", "a")]), gold_fingerprint([("p", "b")])
    check_gold_state(base, base, target)
    check_gold_state(EMPTY, None, target)  # an initial run starts from an empty gold
    with pytest.raises(RuntimeError, match="merged without committing"):
        check_gold_state(target, base, target)
    with pytest.raises(RuntimeError, match="neither"):
        check_gold_state(gold_fingerprint([("p", "z")]), base, target)
    with pytest.raises(RuntimeError, match="neither"):
        check_gold_state(base, None, target)  # an initial run into a gold that is not empty


def test_merge_counts_must_equal_the_diff() -> None:
    check_merge_counts({"num_inserted_rows": 3, "num_deleted_rows": 2, "num_updated_rows": 0}, 3, 2)
    with pytest.raises(RuntimeError, match="inserted/deleted/updated"):
        check_merge_counts({"num_inserted_rows": 3, "num_deleted_rows": 1}, 3, 2)
    with pytest.raises(RuntimeError, match="inserted/deleted/updated"):
        check_merge_counts({"num_inserted_rows": 3, "num_deleted_rows": 2, "num_updated_rows": 1}, 3, 2)


def test_the_canary_compares_with_its_own_model_only() -> None:
    config = EmbeddingConfig()
    canary = Canary(config.model_id, ("a", "b"), ((1.0, 0.0), (0.0, 1.0)))
    assert canary_cosines(canary, config, [[1.0, 0.0], [0.6, 0.8]]) == pytest.approx([1.0, 0.8])
    other = dataclasses.replace(canary, model_id="other@1")
    with pytest.raises(RuntimeError, match="the canary is of"):
        canary_cosines(other, config, [[1.0, 0.0], [0.0, 1.0]])
    assert math.isclose(cosine([3.0, 4.0], [6.0, 8.0]), 1.0)
