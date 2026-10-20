"""The gold table: one row per published chunk with its embedding, the source of the AI Search index (D-014).

Rows hold only what a chunk_id determines (D-011): the chunk's path, section path, text and token count, plus
its vector, the model that made it, and the run that inserted it. A row is never updated: a run inserts the
chunks it added and deletes the ones it removed, and leaves kept chunks alone, so a kept row's
`embedded_by_run_id` shows it was not re-embedded. Positions and tags are not here: they change while a chunk
stays the same (kept chunks move, D-011). A model change is a new gold table and a new chain, not an update.
Pure Python, so the job and the tests share it.
"""

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields

from docs_rag.chunk import breadcrumb
from docs_rag.silver import SilverRecord, fingerprint

# Frozen once the index exists: the index's schema is fixed at creation, and a source-table schema change
# needs the index rebuilt (D-001, D-014). Anything new goes into silver or a side table.
GOLD_COLUMNS: tuple[tuple[str, str], ...] = (
    ("chunk_id", "STRING NOT NULL"),  # the index's primary key
    ("path", "STRING NOT NULL"),
    ("section_path", "ARRAY<STRING> NOT NULL"),
    ("breadcrumb", "STRING NOT NULL"),  # section path joined, for display and citations
    ("body", "STRING NOT NULL"),
    ("embed_text", "STRING NOT NULL"),  # what the model embedded: breadcrumb + body
    ("n_tokens", "INT NOT NULL"),
    ("embedding", "ARRAY<FLOAT> NOT NULL"),
    ("embedding_model", "STRING NOT NULL"),  # model@revision
    ("embedded_by_run_id", "STRING NOT NULL"),  # the run that inserted the row
    ("embedded_at", "TIMESTAMP NOT NULL"),
)
INDEX_COLUMNS = ("chunk_id", "path", "breadcrumb", "body")  # synced to the index beside the vector


def gold_ddl() -> str:
    return ", ".join(f"{name} {kind}" for name, kind in GOLD_COLUMNS)


@dataclass(frozen=True)
class GoldRow:
    chunk_id: str
    path: str
    section_path: tuple[str, ...]
    breadcrumb: str
    body: str
    embed_text: str
    n_tokens: int
    embedding: tuple[float, ...]
    embedding_model: str
    embedded_by_run_id: str
    embedded_at: dt.datetime

    def as_row(self) -> dict[str, object]:
        row: dict[str, object] = {f.name: getattr(self, f.name) for f in fields(self)}
        row["section_path"] = list(self.section_path)
        row["embedding"] = list(self.embedding)
        return row


def gold_rows(
    records: Sequence[SilverRecord],
    vectors: Sequence[Sequence[float]],
    model_id: str,
    run_id: str,
    at: dt.datetime,
) -> list[GoldRow]:
    """Rows for the chunks just embedded; `vectors[i]` is the embedding of `records[i].embed_text`."""
    if len(records) != len(vectors):
        raise ValueError(f"{len(records)} records but {len(vectors)} vectors")
    return [
        GoldRow(
            chunk_id=r.chunk_id,
            path=r.path,
            section_path=r.section_path,
            breadcrumb=breadcrumb(r.section_path),
            body=r.body,
            embed_text=r.embed_text,
            n_tokens=r.n_tokens,
            embedding=tuple(float(x) for x in v),
            embedding_model=model_id,
            embedded_by_run_id=run_id,
            embedded_at=at,
        )
        for r, v in zip(records, vectors, strict=True)
    ]


def gold_fingerprint(pairs: Sequence[tuple[str, str]]) -> str:
    """The fingerprint the run log keeps for a snapshot's chunks: silver.fingerprint of (path, chunk_id)."""
    return fingerprint(pairs)


EMPTY = fingerprint([])


def check_gold_state(gold: str, baseline: str | None, target: str) -> None:
    """Gold must hold exactly the chain head's chunks before a run changes it (D-011): the baseline's logged
    chunk fingerprint, or nothing for an initial run. If it already holds this run's target, an earlier
    attempt got past its MERGE but not to its committed row; there is no automatic resume (D-014): gold is
    repaired by hand to the baseline (a compensating MERGE from the baseline's gold version) and the run
    repeated."""
    expected = baseline if baseline is not None else EMPTY
    if gold == expected:
        return
    if gold == target:
        raise RuntimeError(
            "gold already holds this run's target but the chain head is its baseline: an earlier attempt "
            "merged without committing; repair gold to the baseline, then run again"
        )
    raise RuntimeError(
        f"gold ({gold[:12]}) holds neither the chain head ({expected[:12]}) nor this run's target"
    )


def check_merge_counts(metrics: dict[str, int], inserted: int, deleted: int) -> None:
    """The MERGE's own metrics (num_inserted_rows, num_deleted_rows) must equal the chunk diff; it updates
    nothing (rows are immutable)."""
    got = (
        metrics.get("num_inserted_rows"),
        metrics.get("num_deleted_rows"),
        metrics.get("num_updated_rows", 0),
    )
    if got != (inserted, deleted, 0):
        raise RuntimeError(f"MERGE inserted/deleted/updated {got}, the diff says ({inserted}, {deleted}, 0)")


# --- the MERGE ---------------------------------------------------------------------------------------------


def merge_source_ddl() -> str:
    """The MERGE's source: an op ('insert' or 'delete') and the gold columns, nullable (a delete row carries
    only its chunk_id)."""
    return ", ".join(
        ["op STRING"] + [f"{name} {kind.removesuffix(' NOT NULL')}" for name, kind in GOLD_COLUMNS]
    )


def merge_source_rows(inserts: Sequence[GoldRow], deletes: Sequence[str]) -> list[dict[str, object]]:
    empty: dict[str, object] = {name: None for name, _ in GOLD_COLUMNS}
    rows = [{"op": "insert", **r.as_row()} for r in inserts]
    rows += [{"op": "delete", **empty, "chunk_id": chunk_id} for chunk_id in deletes]
    ids = [r["chunk_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("a chunk_id is both inserted and deleted, or listed twice")
    return rows


_COLUMNS = ", ".join(name for name, _ in GOLD_COLUMNS)
MERGE_GOLD = f"""
MERGE INTO IDENTIFIER(:table) AS g
USING gold_changes AS c
ON g.chunk_id = c.chunk_id
WHEN MATCHED AND c.op = 'delete' THEN DELETE
WHEN NOT MATCHED AND c.op = 'insert' THEN INSERT ({_COLUMNS})
  VALUES ({", ".join(f"c.{name}" for name, _ in GOLD_COLUMNS)})
"""


def merge_version(history: Sequence[tuple[int, str]], before: int) -> int:
    """The version the MERGE wrote, from the (version, operation) rows of the table history after `before`:
    exactly one MERGE; anything else there may only be Databricks' own OPTIMIZE (D-010)."""
    new = [(v, op) for v, op in history if v > before]
    merges = [v for v, op in new if op == "MERGE"]
    others = sorted({op for _, op in new if op not in ("MERGE", "OPTIMIZE")})
    if len(merges) != 1 or others:
        raise RuntimeError(f"gold history after version {before} is not one MERGE: {sorted(new)}")
    return merges[0]


def history_merge_metrics(operation_metrics: Mapping[str, str]) -> dict[str, int]:
    """DESCRIBE HISTORY's metrics of a MERGE, named as the MERGE statement's own result names them."""
    names = {
        "num_inserted_rows": "numTargetRowsInserted",
        "num_deleted_rows": "numTargetRowsDeleted",
        "num_updated_rows": "numTargetRowsUpdated",
    }
    return {ours: int(operation_metrics.get(theirs, "-1")) for ours, theirs in names.items()}
