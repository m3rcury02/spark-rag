"""Databricks job: chunk one bronze tag into silver_chunks under one chunk config.

Runs as a serverless spark_python_task (see scripts/run_job.py). Steps:
1. Check the environment (docs_rag.jobenv), then read the tag's bronze rows and check the snapshot is whole:
   rows present, one commit, unique paths, every content hash matching (docs_rag.silver.bronze_violations).
2. Clean and chunk every doc (docs_rag.chunk) and check the result before writing: no chunk over max_tokens,
   no empty chunk, unique chunk_ids, no markup left after cleaning, and no doc without chunks unless it is in
   the expected-zero set (D-008, D-010).
3. Read what silver holds for (config_id, tag), recompute every stored row's chunk_id and n_tokens from its
   own content, and compare with the fresh chunking. Stored and identical: the MERGE is skipped (no table
   version). Stored and different: the run fails before writing.
4. Insert-only MERGE on (config_id, source_repo, version_tag, chunk_id); it must insert every row and touch
   nothing else.
5. Read the tag back and check it again, then compare docs whose content is byte-identical in other tags
   chunked under the same config: they must have the same chunk_ids (determinism, no tag leaking into IDs).

The summary holds a fingerprint of the (path, chunk_id) set, so a local recompute can prove the job and a
laptop chunk identically (scripts/m2_verify_silver.py).
"""

import argparse
import datetime as dt
import time
from typing import Any

from pyspark.sql import SparkSession

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig
from docs_rag.config import Config, validate_tag
from docs_rag.jobenv import base_parser, check_environment, run_with_summary
from docs_rag.silver import (
    BronzeDoc,
    StoredChunk,
    bronze_violations,
    check_against_stored,
    chunk_docs,
    cross_tag_violations,
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
SELECT path, commit_sha, doc_content_sha256, chunk_id, occurrence, chunk_index, section_path, body,
       embed_text, n_tokens, chunker_version, max_tokens, overlap_tokens
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
        "chunker_version": CHUNKER_VERSION,
        "max_tokens": chunking.max_tokens,
        "overlap_tokens": chunking.overlap_tokens,
    }
    summary["environment"] = check_environment(args.expect_env, args.expect_version)

    spark = SparkSession.builder.getOrCreate()
    summary["environment"]["spark"] = spark.version
    table = config.silver_table
    spark.sql(CREATE_SILVER, args={"table": table})

    # 1. The bronze snapshot must be whole before anything is derived from it.
    bronze_rows = spark.sql(
        "SELECT path, content, content_sha256, sha2(content, 256) AS content_sha256_actual, commit_sha "
        "FROM IDENTIFIER(:table) WHERE source_repo = :repo AND version_tag = :tag",
        args={"table": config.bronze_table, "repo": config.source_repo, "tag": tag},
    ).collect()
    docs = [BronzeDoc(**r.asDict()) for r in bronze_rows]
    problems = bronze_violations(docs)
    summary["bronze"] = {
        "docs": len(docs),
        "commit_sha": docs[0].commit_sha if docs else None,
        "violations": problems,
    }
    if problems:
        raise RuntimeError("bronze snapshot is not whole: " + "; ".join(problems))

    # 2. Chunk, and check the chunking before writing anything.
    t0 = time.monotonic()
    chunked = chunk_docs(docs, chunking)
    records = to_records(
        docs, chunked, config.source_repo, tag, chunking, args.run_id, dt.datetime.now(dt.UTC)
    )
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
    key = {"config_id": chunking.config_id, "repo": config.source_repo, "tag": tag}
    stored = read_stored(spark, table, key)
    problems = stored_violations(stored, chunking)
    check = check_against_stored(records, stored)
    summary["check"] = {**check.__dict__, "stored_row_violations": problems}
    if problems:
        raise RuntimeError("stored silver rows fail their recheck: " + "; ".join(problems))
    if check.status == "conflict":
        raise RuntimeError(
            f"{tag} is already chunked under config {chunking.config_id} and the fresh chunking differs "
            f"({check.only_stored} rows only stored, {check.only_computed} only computed): the chunker "
            "changed without a CHUNKER_VERSION bump, or bronze changed"
        )

    # 4. Insert-only MERGE, skipped when silver already holds exactly this chunking.
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

    # 5. Read back and check again; then the cross-tag determinism check.
    problems = []
    if check.status == "new":
        merge = summary["merge"]
        if (merge["num_inserted_rows"], merge["num_updated_rows"], merge["num_deleted_rows"]) != (
            len(records),
            0,
            0,
        ):
            problems.append(f"MERGE metrics {merge}, expected {len(records)} inserted and nothing else")
    after = read_stored(spark, table, key)
    problems += stored_violations(after, chunking)
    reread = check_against_stored(records, after)
    if reread.status != "identical":
        problems.append(f"read-back differs from the chunking: {reread}")
    stored_fingerprint = fingerprint((r.path, r.chunk_id) for r in after)
    if stored_fingerprint != summary["chunking"]["fingerprint"]:
        problems.append("read-back fingerprint differs from the computed one")

    others = spark.sql(
        "SELECT version_tag, path, doc_content_sha256, chunk_id FROM IDENTIFIER(:table) "
        "WHERE config_id = :config_id AND source_repo = :repo AND version_tag <> :tag",
        args={"table": table, **key},
    ).collect()
    compared, cross = cross_tag_violations(records, (tuple(r) for r in others))
    summary["cross_tag"] = {"docs_compared": compared, "violations": cross}
    problems += cross

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
