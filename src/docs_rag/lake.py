"""Thin Spark adapter shared by the Databricks jobs: reading bronze and silver, and re-checking a stored
chunking against a fresh one. It never imports pyspark (the session is passed in), so the wheel and the unit
tests stay JVM-free; the logic it calls lives in docs_rag.silver.
"""

import datetime as dt
from typing import Any

from docs_rag.chunk import ChunkConfig, DocChunks
from docs_rag.config import Config
from docs_rag.silver import (
    BronzeDoc,
    SilverRecord,
    StoredCheck,
    StoredChunk,
    bronze_violations,
    check_against_stored,
    chunk_docs,
    stored_baseline_violations,
    stored_violations,
    to_records,
)

STORED = """
SELECT path, commit_sha, doc_content_sha256, doc_clean_sha256, chunk_id, occurrence, chunk_index,
       section_path, body, embed_text, n_tokens, char_start, char_end, cleaner_version, chunker_version,
       max_tokens, overlap_tokens
FROM IDENTIFIER(:table)
WHERE config_id = :config_id AND source_repo = :repo AND version_tag = :tag
"""


def latest_version(spark: Any, table: str) -> int:
    row = spark.sql("DESCRIBE HISTORY IDENTIFIER(:table) LIMIT 1", args={"table": table}).collect()[0]
    return int(row["version"])


def read_stored(spark: Any, table: str, args: dict[str, str]) -> list[StoredChunk]:
    return [
        StoredChunk(**{**r.asDict(), "section_path": tuple(r["section_path"])})
        for r in spark.sql(STORED, args={"table": table, **args}).collect()
    ]


def read_bronze(spark: Any, config: Config, tag: str) -> list[BronzeDoc]:
    """The tag's bronze rows; raises unless the snapshot is whole (docs_rag.silver.bronze_violations)."""
    rows = spark.sql(
        "SELECT path, content, content_sha256, sha2(content, 256) AS content_sha256_actual, commit_sha "
        "FROM IDENTIFIER(:table) WHERE source_repo = :repo AND version_tag = :tag",
        args={"table": config.bronze_table, "repo": config.source_repo, "tag": tag},
    ).collect()
    docs = [BronzeDoc(**r.asDict()) for r in rows]
    problems = bronze_violations(docs)
    if problems:
        raise RuntimeError(f"bronze snapshot of {tag} is not whole: " + "; ".join(problems))
    return docs


def compare_with_stored(
    spark: Any, config: Config, chunking: ChunkConfig, tag: str, records: list[SilverRecord]
) -> tuple[StoredCheck, list[StoredChunk]]:
    """Recheck the stored rows of (chunking's config_id, tag) and compare them with a fresh chunking."""
    key = {"config_id": chunking.config_id, "repo": config.source_repo, "tag": tag}
    stored = read_stored(spark, config.silver_table, key)
    problems = stored_violations(stored, chunking)
    if problems:
        raise RuntimeError(f"stored silver rows of {tag} fail their recheck: " + "; ".join(problems))
    return check_against_stored(records, stored), stored


def chunk_bronze(
    config: Config, chunking: ChunkConfig, tag: str, docs: list[BronzeDoc], run_id: str
) -> tuple[list[DocChunks], list[SilverRecord]]:
    chunked = chunk_docs(docs, chunking)
    records = to_records(docs, chunked, config.source_repo, tag, chunking, run_id, dt.datetime.now(dt.UTC))
    return chunked, records


def stored_baseline(
    spark: Any, config: Config, tag: str, config_id: str
) -> tuple[list[BronzeDoc], list[StoredChunk]]:
    """The bronze snapshot of `tag` and its stored silver rows under `config_id`, a config today's code cannot
    re-chunk, after docs_rag.silver.stored_baseline_violations (D-013's re-baseline path)."""
    docs = read_bronze(spark, config, tag)
    stored = read_stored(
        spark, config.silver_table, {"config_id": config_id, "repo": config.source_repo, "tag": tag}
    )
    problems = stored_baseline_violations(stored, docs, config_id, config.zero_chunk_docs)
    if problems:
        raise RuntimeError(
            f"stored silver of the baseline {tag} under {config_id} fails its checks: " + "; ".join(problems)
        )
    return docs, stored
