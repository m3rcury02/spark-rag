"""Databricks job: load one tag of the source docs into bronze_docs.

Runs as a serverless spark_python_task (see scripts/run_job.py). Steps:
1. Check the environment is the pinned one (Python, serverless environment version, Databricks Connect) and
   that the installed docs_rag is exactly the wheel built for this run (D-005).
2. Fetch the tag's source archive and read a complete snapshot of its docs (docs_rag.fetch).
3. Compare it with what bronze already holds for the tag (docs_rag.bronze.check_against_stored). A tag that
   is already stored must match exactly; any difference stops the run before anything is written (D-006).
4. Insert-only MERGE on (source_repo, version_tag, path), so re-running a tag inserts nothing.
5. Record the MERGE result, the table commit it made (if any) and per-tag invariants, including
   sha2(content) = content_sha256 for every row.

The summary JSON is written to <summary-dir>/<run-id>.json whether the run succeeds or fails.
"""

import argparse
import datetime as dt
import importlib.metadata
import json
import os
import platform
import sys
import time
from typing import Any

from pyspark.sql import SparkSession

from docs_rag.bronze import StoredDoc, bronze_schema_ddl, check_against_stored, to_records
from docs_rag.config import Config, validate_tag
from docs_rag.fetch import archive_url, download, read_snapshot

# Databricks Connect major version per serverless environment version, from the environment release notes
# (https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/, updated 2026-10-12).
DBCONNECT_MAJOR = {"3": "16", "4": "17", "5": "18", "6": "19"}

CREATE_BRONZE = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({bronze_schema_ddl()})
COMMENT 'Raw source docs: one immutable snapshot per (source_repo, version_tag). See D-006.'
"""

# Insert-only: rows of a stored tag are never updated or deleted here (D-006). Re-running inserts nothing.
MERGE_BRONZE = """
MERGE INTO IDENTIFIER(:table) AS t
USING bronze_incoming AS s
ON t.source_repo = s.source_repo AND t.version_tag = s.version_tag AND t.path = s.path
WHEN NOT MATCHED THEN INSERT *
"""

TAG_INVARIANTS = """
SELECT version_tag,
       count(*) AS docs,
       count(DISTINCT path) AS distinct_paths,
       sum(size_bytes) AS bytes,
       count(DISTINCT commit_sha) AS commits,
       min(commit_sha) AS commit_sha,
       count_if(sha2(content, 256) <> content_sha256) AS hash_mismatches,
       count_if(octet_length(content) <> size_bytes) AS size_mismatches
FROM IDENTIFIER(:table)
WHERE source_repo = :repo
GROUP BY version_tag
ORDER BY version_tag
"""


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def check_environment(expect_env: str, expect_version: str) -> dict[str, Any]:
    env = {
        "python": platform.python_version(),
        "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
        "databricks_connect": importlib.metadata.version("databricks-connect"),
        "docs_rag": importlib.metadata.version("docs-rag"),
        "machine": platform.machine(),
    }
    want_connect = DBCONNECT_MAJOR.get(expect_env)
    problems = []
    if sys.version_info[:2] != (3, 12):
        problems.append(f"Python {env['python']}, expected 3.12")
    if not str(env["databricks_runtime_version"]).startswith(f"client.{expect_env}."):
        problems.append(f"runtime {env['databricks_runtime_version']}, expected client.{expect_env}.*")
    if env["databricks_connect"].split(".")[0] != want_connect:
        problems.append(f"databricks-connect {env['databricks_connect']}, expected {want_connect}.x")
    if env["docs_rag"] != expect_version:
        problems.append(f"docs_rag {env['docs_rag']} installed, expected {expect_version} (stale cache?)")
    if problems:
        raise RuntimeError("environment check failed: " + "; ".join(problems))
    return env


def latest_version(spark: SparkSession, table: str) -> int:
    row = spark.sql("DESCRIBE HISTORY IDENTIFIER(:table) LIMIT 1", args={"table": table}).collect()[0]
    return int(row["version"])


def ingest(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    config = Config(catalog=args.catalog, schema=args.schema)
    tag = validate_tag(args.tag)
    summary["environment"] = check_environment(args.expect_env, args.expect_version)

    url = archive_url(config.source_repo, tag)
    t0 = time.monotonic()
    data = download(url)
    summary["download"] = {"url": url, "bytes": len(data), "seconds": round(time.monotonic() - t0, 1)}
    snap = read_snapshot(data, config, tag)
    summary["snapshot"] = {
        "commit_sha": snap.commit_sha,
        "docs": len(snap.docs),
        "nested_docs": sum(d.path.count("/") > 1 for d in snap.docs),
        "bytes": sum(d.size_bytes for d in snap.docs),
        "largest_bytes": max(d.size_bytes for d in snap.docs),
        "docs_with_crlf": sum("\r\n" in d.content for d in snap.docs),
        "docs_with_bom": sum(d.content.startswith("﻿") for d in snap.docs),
    }

    spark = SparkSession.builder.getOrCreate()
    summary["environment"]["spark"] = spark.version
    table = config.bronze_table
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema}")  # names validated by Config
    spark.sql(CREATE_BRONZE, args={"table": table})

    stored_rows = spark.sql(
        "SELECT path, content_sha256, commit_sha FROM IDENTIFIER(:table) "
        "WHERE source_repo = :repo AND version_tag = :tag",
        args={"table": table, "repo": config.source_repo, "tag": tag},
    ).collect()
    stored = (StoredDoc(r["path"], r["content_sha256"], r["commit_sha"]) for r in stored_rows)
    check = check_against_stored(snap, stored)
    summary["check"] = {"status": check.status, "stored": check.stored_docs, "source": check.source_docs}
    if check.status == "conflict":
        summary["check"]["detail"] = check.describe()
        raise RuntimeError(f"{tag} is already in bronze and differs from the source: {check.describe()}")

    version_before = latest_version(spark, table)
    records = to_records(snap, args.run_id, dt.datetime.now(dt.UTC))
    incoming = spark.createDataFrame([r.as_row() for r in records], bronze_schema_ddl())
    incoming.createOrReplaceTempView("bronze_incoming")
    summary["merge"] = spark.sql(MERGE_BRONZE, args={"table": table}).collect()[0].asDict()
    version_after = latest_version(spark, table)

    commits = []
    if version_after != version_before:  # attach history only for commits this MERGE made
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
    rows = spark.sql(TAG_INVARIANTS, args={"table": table, "repo": config.source_repo}).collect()
    summary["tags_after"] = {
        r["version_tag"]: {k: v for k, v in r.asDict().items() if k != "version_tag"} for r in rows
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--summary-dir", required=True)
    ap.add_argument("--expect-env", required=True, help="serverless environment version the job must run on")
    ap.add_argument("--expect-version", required=True, help="docs_rag version the job must import")
    args = ap.parse_args()

    summary: dict[str, Any] = {"run_id": args.run_id, "tag": args.tag, "started_utc": utc_now()}
    try:
        ingest(args, summary)
        summary["status"] = "succeeded"
    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        summary["finished_utc"] = utc_now()
        os.makedirs(args.summary_dir, exist_ok=True)
        with open(os.path.join(args.summary_dir, f"{args.run_id}.json"), "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
