"""Databricks job: chunk one bronze tag into silver_chunks under one chunk config.

Runs as a serverless spark_python_task (see scripts/run_job.py). Steps:
1. Check the environment (docs_rag.jobenv) and that the chunk config is one of Config.chunk_grid. Read the
   tag's bronze rows and check the snapshot is whole: rows present, one commit, unique paths, every content
   hash matching (docs_rag.silver.bronze_violations).
2. Clean and chunk every doc (docs_rag.chunk) and check the result before writing: no chunk over max_tokens,
   no empty chunk, unique chunk_ids, no markup left after cleaning, and no doc without chunks unless it is in
   the expected-zero set (D-008, D-010).
3. Read what silver holds for (config_id, tag), recompute every stored row's chunk_id and n_tokens from its
   own content, and compare with the fresh chunking. Stored and identical: the MERGE is skipped (no table
   version). Stored and different: the run fails before writing.
4. Re-chunk every other tag that silver holds under this config_id from its bronze rows; each must reproduce
   its stored rows exactly. So code that changed without a CLEANER_VERSION/CHUNKER_VERSION bump fails here,
   before it can add a tag chunked by different code under the same config_id.
5. Insert-only MERGE on (config_id, source_repo, version_tag, chunk_id); it must insert every row and touch
   nothing else. Then read the tag back and check it again.

The summary holds a fingerprint of the (path, chunk_id) set and the machine it ran on, so a local recompute
can show the job and a laptop chunk identically (scripts/m2_verify_silver.py).
"""

import argparse
import datetime as dt
import time
from typing import Any

from pyspark.sql import SparkSession

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig, DocChunks
from docs_rag.clean import CLEANER_VERSION
from docs_rag.config import Config, validate_tag
from docs_rag.jobenv import base_parser, check_environment, run_with_summary
from docs_rag.silver import (
    BronzeDoc,
    SilverRecord,
    StoredCheck,
    StoredChunk,
    bronze_violations,
    check_against_stored,
    chunk_docs,
    conflict_reason,
    fingerprint,
    record_violations,
    silver_schema_ddl,
    stored_violations,
    to_records,
    token_stats,
)

CREATE_SILVER = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({silver_schema_ddl()})
COMMENT 'Chunks of bronze docs: one immutable chunking per (config_id, source_repo, version_tag). See D-010.'
"""

# Insert-only: a stored chunking is never updated or deleted here (D-010). Re-running inserts nothing.
MERGE_SILVER = """
MERGE INTO IDENTIFIER(:table) AS t
USING silver_incoming AS s
ON t.config_id = s.config_id AND t.source_repo = s.source_repo AND t.version_tag = s.version_tag
   AND t.chunk_id = s.chunk_id
WHEN NOT MATCHED THEN INSERT *
"""

STORED = """
SELECT path, commit_sha, doc_content_sha256, doc_clean_sha256, chunk_id, occurrence, chunk_index,
       section_path, body, embed_text, n_tokens, char_start, char_end, cleaner_version, chunker_version,
       max_tokens, overlap_tokens
FROM IDENTIFIER(:table)
WHERE config_id = :config_id AND source_repo = :repo AND version_tag = :tag
"""

TAGS_AFTER = """
SELECT version_tag,
       count(*) AS chunks,
       count(DISTINCT chunk_id) AS distinct_chunk_ids,
       count(DISTINCT path) AS docs,
       count(DISTINCT commit_sha) AS commits,
       max(n_tokens) AS max_n_tokens,
       count_if(n_tokens > max_tokens) AS chunks_over_max
FROM IDENTIFIER(:table)
WHERE config_id = :config_id AND source_repo = :repo
GROUP BY version_tag
ORDER BY version_tag
"""


def latest_version(spark: SparkSession, table: str) -> int:
    row = spark.sql("DESCRIBE HISTORY IDENTIFIER(:table) LIMIT 1", args={"table": table}).collect()[0]
    return int(row["version"])


def read_stored(spark: SparkSession, table: str, args: dict[str, str]) -> list[StoredChunk]:
    return [
        StoredChunk(**{**r.asDict(), "section_path": tuple(r["section_path"])})
        for r in spark.sql(STORED, args={"table": table, **args}).collect()
    ]


def read_bronze(spark: SparkSession, config: Config, tag: str) -> list[BronzeDoc]:
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
    spark: SparkSession, config: Config, tag: str, records: list[SilverRecord]
) -> tuple[StoredCheck, list[StoredChunk]]:
    """Recheck the stored rows of (config_id, tag) and compare them with a fresh chunking."""
    key = {"config_id": config.chunk.config_id, "repo": config.source_repo, "tag": tag}
    stored = read_stored(spark, config.silver_table, key)
    problems = stored_violations(stored, config.chunk)
    if problems:
        raise RuntimeError(f"stored silver rows of {tag} fail their recheck: " + "; ".join(problems))
    return check_against_stored(records, stored), stored


def chunk_bronze(
    config: Config, tag: str, docs: list[BronzeDoc], run_id: str
) -> tuple[list[DocChunks], list[SilverRecord]]:
    chunked = chunk_docs(docs, config.chunk)
    records = to_records(
        docs, chunked, config.source_repo, tag, config.chunk, run_id, dt.datetime.now(dt.UTC)
    )
    return chunked, records


def chunk_tag(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    config = Config(
        catalog=args.catalog,
        schema=args.schema,
        chunk=ChunkConfig(max_tokens=args.max_tokens, overlap_tokens=args.overlap_tokens),
    )
    chunking = config.chunk
    tag = validate_tag(args.tag)
    summary["tag"] = tag
    summary["chunk_config"] = {
        "config_id": chunking.config_id,
        "cleaner_version": CLEANER_VERSION,
        "chunker_version": CHUNKER_VERSION,
        "max_tokens": chunking.max_tokens,
        "overlap_tokens": chunking.overlap_tokens,
    }
    if chunking not in config.chunk_grid:  # an off-grid chunking would stay in silver for good
        raise ValueError(f"{chunking} is not one of Config.chunk_grid {config.chunk_grid}")
    summary["environment"] = check_environment(args.expect_env, args.expect_version)

    spark = SparkSession.builder.getOrCreate()
    summary["environment"]["spark"] = spark.version
    table = config.silver_table
    spark.sql(CREATE_SILVER, args={"table": table})

    # 1. The bronze snapshot must be whole before anything is derived from it.
    docs = read_bronze(spark, config, tag)
    summary["bronze"] = {"docs": len(docs), "commit_sha": docs[0].commit_sha}

    # 2. Chunk, and check the chunking before writing anything.
    t0 = time.monotonic()
    chunked, records = chunk_bronze(config, tag, docs, args.run_id)
    summary["chunking"] = {
        "seconds": round(time.monotonic() - t0, 1),
        "fingerprint": fingerprint((r.path, r.chunk_id) for r in records),
        **token_stats(records, chunked),
    }
    problems = record_violations(records, chunked, chunking, config.zero_chunk_docs)
    summary["chunking"]["violations"] = problems
    if problems:
        raise RuntimeError("chunking failed its checks: " + "; ".join(problems))

    # 3. Compare with what silver already holds for this config and tag.
    check, stored = compare_with_stored(spark, config, tag, records)
    summary["check"] = check.__dict__
    if check.status == "conflict":
        raise RuntimeError(
            f"{tag} is already chunked under config {chunking.config_id} and the fresh chunking differs "
            f"({check.only_stored} rows only stored, {check.only_computed} only computed): "
            + conflict_reason(docs, stored, config.zero_chunk_docs)
        )

    # 4. The current code must reproduce every other tag stored under this config_id.
    others = spark.sql(
        "SELECT DISTINCT version_tag FROM IDENTIFIER(:table) "
        "WHERE config_id = :config_id AND source_repo = :repo AND version_tag <> :tag ORDER BY version_tag",
        args={"table": table, "config_id": chunking.config_id, "repo": config.source_repo, "tag": tag},
    ).collect()
    summary["other_tags_reproduced"] = {}
    for row in others:
        other = row["version_tag"]
        other_docs = read_bronze(spark, config, other)
        _, other_records = chunk_bronze(config, other, other_docs, args.run_id)
        other_check, other_stored = compare_with_stored(spark, config, other, other_records)
        summary["other_tags_reproduced"][other] = other_check.__dict__
        if other_check.status != "identical":
            raise RuntimeError(
                f"the current code does not reproduce {other} as stored under config {chunking.config_id} "
                f"({other_check.only_stored} rows only stored, {other_check.only_computed} only computed): "
                + conflict_reason(other_docs, other_stored, config.zero_chunk_docs)
            )

    # 5. Insert-only MERGE, skipped when silver already holds exactly this chunking; then read back.
    version_before = latest_version(spark, table)
    if check.status == "identical":
        summary["merge"] = "skipped: chunking identical to silver"
    else:
        incoming = spark.createDataFrame([r.as_row() for r in records], silver_schema_ddl())
        incoming.createOrReplaceTempView("silver_incoming")
        summary["merge"] = spark.sql(MERGE_SILVER, args={"table": table}).collect()[0].asDict()
    version_after = latest_version(spark, table)
    commits = []
    if version_after != version_before:
        history = spark.sql("DESCRIBE HISTORY IDENTIFIER(:table)", args={"table": table})
        for r in history.where(f"version > {version_before} AND version <= {version_after}").collect():
            commits.append(
                {"version": r["version"], "operation": r["operation"], "metrics": r["operationMetrics"]}
            )
    summary["table"] = {
        "name": table,
        "version_before_merge": version_before,
        "version_after_merge": version_after,
        "merge_made_a_commit": version_after != version_before,
        "commits": commits,
    }

    problems = []
    if check.status == "new":
        merge = summary["merge"]
        if (merge["num_inserted_rows"], merge["num_updated_rows"], merge["num_deleted_rows"]) != (
            len(records),
            0,
            0,
        ):
            problems.append(f"MERGE metrics {merge}, expected {len(records)} inserted and nothing else")
    reread, after = compare_with_stored(spark, config, tag, records)
    if reread.status != "identical":
        problems.append(f"read-back differs from the chunking: {reread}")
    if fingerprint((r.path, r.chunk_id) for r in after) != summary["chunking"]["fingerprint"]:
        problems.append("read-back fingerprint differs from the computed one")
    rows = spark.sql(
        TAGS_AFTER, args={"table": table, "config_id": chunking.config_id, "repo": config.source_repo}
    ).collect()
    summary["tags_after"] = {
        r["version_tag"]: {k: v for k, v in r.asDict().items() if k != "version_tag"} for r in rows
    }
    for t, stats in summary["tags_after"].items():
        if (
            stats["chunks"] != stats["distinct_chunk_ids"]
            or stats["commits"] != 1
            or stats["chunks_over_max"]
        ):
            problems.append(f"{t}: {stats}")
    summary["invariant_violations"] = problems
    if problems:
        raise RuntimeError("silver checks failed after the write: " + "; ".join(problems))


def main() -> None:
    parser = base_parser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--max-tokens", type=int, default=ChunkConfig.max_tokens)
    parser.add_argument("--overlap-tokens", type=int, default=ChunkConfig.overlap_tokens)
    run_with_summary(chunk_tag, parser)


if __name__ == "__main__":
    main()
