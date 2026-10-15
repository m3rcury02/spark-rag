"""Databricks job: load one tag of the source docs into bronze_docs with an idempotent MERGE.

Runs as a serverless spark_python_task. The docs_rag wheel comes from the job environment, which pins the
serverless environment version (D-005). Re-running a tag must report 0 inserted, 0 updated, 0 deleted.

Writes a JSON summary to <summary-dir>/<ingest_run_id>.json (a UC Volume path) and prints it.
"""

import argparse
import datetime as dt
import io
import json
import os
import platform
import sys
import time
import uuid
from typing import Any

from pyspark.sql import SparkSession

from docs_rag.config import Config, validate_tag
from docs_rag.fetch import archive_url, download, read_snapshot

BRONZE_SCHEMA = (
    "source_repo STRING NOT NULL, version_tag STRING NOT NULL, commit_sha STRING, path STRING NOT NULL, "
    "content STRING NOT NULL, content_sha256 STRING NOT NULL, size_bytes INT NOT NULL, "
    "ingested_at TIMESTAMP NOT NULL, ingest_run_id STRING NOT NULL"
)

CREATE_BRONZE = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({BRONZE_SCHEMA})
COMMENT 'Raw source docs, one row per (source_repo, version_tag, path). Written by jobs/bronze_ingest.py'
"""

# The delete branch is scoped to the repo and tag being loaded, so loading one tag never touches another.
MERGE_BRONZE = """
MERGE INTO IDENTIFIER(:table) AS t
USING bronze_incoming AS s
ON t.source_repo = s.source_repo AND t.version_tag = s.version_tag AND t.path = s.path
WHEN MATCHED AND (t.content_sha256 <> s.content_sha256 OR t.commit_sha IS DISTINCT FROM s.commit_sha) THEN
  UPDATE SET t.commit_sha = s.commit_sha, t.content = s.content, t.content_sha256 = s.content_sha256,
             t.size_bytes = s.size_bytes, t.ingested_at = s.ingested_at, t.ingest_run_id = s.ingest_run_id
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.source_repo = :repo AND t.version_tag = :tag THEN DELETE
"""


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema)
    ap.add_argument("--summary-dir", required=True)
    args = ap.parse_args()

    config = Config(catalog=args.catalog, schema=args.schema)
    tag = validate_tag(args.tag)
    run_id = str(uuid.uuid4())
    summary: dict[str, Any] = {"ingest_run_id": run_id, "tag": tag, "started_utc": utc_now()}
    summary["runtime"] = {
        "python": platform.python_version(),
        "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
        "machine": platform.machine(),
    }
    if sys.version_info[:2] != (3, 12):  # D-005: the pinned environment must give us 3.12
        raise SystemExit(f"expected Python 3.12, got {platform.python_version()}")

    url = archive_url(config.source_repo, tag)
    t0 = time.monotonic()
    data = download(url)
    summary["download"] = {"url": url, "bytes": len(data), "seconds": round(time.monotonic() - t0, 1)}

    snap = read_snapshot(io.BytesIO(data), config, tag)
    sizes = [d.size_bytes for d in snap.docs]
    summary["snapshot"] = {
        "commit_sha": snap.commit_sha,
        "docs": len(snap.docs),
        "nested_docs": sum(d.path.count("/") > 1 for d in snap.docs),
        "total_bytes": sum(sizes),
        "largest_bytes": max(sizes, default=0),
        "docs_with_crlf": sum("\r\n" in d.content for d in snap.docs),
        "docs_with_bom": sum(d.content.startswith("﻿") for d in snap.docs),
    }

    spark = SparkSession.builder.getOrCreate()
    summary["runtime"]["spark"] = spark.version
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema}")
    spark.sql(CREATE_BRONZE, args={"table": config.bronze_table})

    ingested_at = dt.datetime.now(dt.UTC)
    rows = [
        (
            snap.source_repo,
            tag,
            snap.commit_sha,
            d.path,
            d.content,
            d.content_sha256,
            d.size_bytes,
            ingested_at,
            run_id,
        )
        for d in snap.docs
    ]
    spark.createDataFrame(rows, BRONZE_SCHEMA).createOrReplaceTempView("bronze_incoming")
    params = {"table": config.bronze_table, "repo": config.source_repo, "tag": tag}
    summary["merge"] = spark.sql(MERGE_BRONZE, args=params).collect()[0].asDict()

    history = spark.sql("DESCRIBE HISTORY IDENTIFIER(:table) LIMIT 1", args={"table": config.bronze_table})
    last = history.collect()[0].asDict()
    summary["table_commit"] = {
        "version": last["version"],
        "operation": last["operation"],
        "operationMetrics": last["operationMetrics"],
    }
    counts = spark.sql(
        "SELECT version_tag, count(*) AS docs, sum(size_bytes) AS bytes, count(DISTINCT path) AS paths "
        "FROM IDENTIFIER(:table) GROUP BY version_tag ORDER BY version_tag",
        args={"table": config.bronze_table},
    )
    summary["table_after"] = {
        r["version_tag"]: {k: r[k] for k in ("docs", "bytes", "paths")} for r in counts.collect()
    }
    summary["finished_utc"] = utc_now()

    text = json.dumps(summary, indent=2, default=str)
    os.makedirs(args.summary_dir, exist_ok=True)
    with open(os.path.join(args.summary_dir, f"{run_id}.json"), "w") as f:
        f.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
