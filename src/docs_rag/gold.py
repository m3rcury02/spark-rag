"""The gold table: one row per published chunk with its embedding, the source of the AI Search index (D-014).

Rows hold only what a chunk_id determines (D-011): the chunk's path, section path, text and token count, plus
its vector, the model that made it, and the run that inserted it. A row is never updated: a run inserts the
chunks it added and deletes the ones it removed, and leaves kept chunks alone, so a kept row's
`embedded_by_run_id` shows it was not re-embedded. Positions and tags are not here: they change while a chunk
stays the same (kept chunks move, D-011). A model change is a new gold table and a new chain, not an update.
Pure Python, so the job and the tests share it.
"""

import datetime as dt
from collections.abc import Sequence
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
