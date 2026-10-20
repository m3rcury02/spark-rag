"""Databricks job: bring the AI Search index to gold's committed state, and prove it got there (D-014).

    uv run --with databricks-sdk==0.152.0 python scripts/run_job.py --script jobs/sync_index.py \
        --label <label>

Runs after change_detect has committed: the index is a replica of gold, so a sync that fails leaves the run
committed and is simply run again. Steps:
1. Check the environment. Gold must hold exactly the head of the committed chain of the gold sink (its logged
   chunk fingerprint): only a committed state is ever synced, never one a failed run left behind.
2. The index (Config.index, created by scripts/m5_index.py) must have gold as its source. Wait until no update
   of its sync pipeline is running.
3. List every key of the index (scan). Equal to gold's chunk_ids: the index is current and no sync is made
   (gold rows are immutable, so equal keys mean equal rows). Otherwise trigger one sync, wait for its own
   pipeline update to complete and the index to report nothing pending ("ready" is not "synced", D-001), and
   list the keys again: they must now equal gold's, and indexed_row_count gold's row count.
4. Probe: query the index with the vectors of a few gold rows (the smallest chunk_ids); each row must come
   back among its own top 5. A mechanics check (the vector column is wired), not retrieval quality (M6).
Writes no table.
"""

import argparse
from typing import Any

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from docs_rag.changes import LogEntry, pick_baseline
from docs_rag.config import Config
from docs_rag.gold import gold_fingerprint
from docs_rag.index_sync import (
    compare_keys,
    index_info,
    query_keys,
    scan_keys,
    summary_of,
    sync,
    wait_idle,
)
from docs_rag.jobenv import base_parser, check_environment, run_with_summary

PROBES = 3
TOP_K = 5


def sync_index(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    config = Config(catalog=args.catalog, schema=args.schema)
    gold = config.gold_table
    if gold is None:
        raise RuntimeError("Config.sink is 'none': there is no gold table to sync from")
    summary.update({"gold": gold, "index": config.index})
    summary["environment"] = check_environment(args.expect_env, args.expect_version)
    spark = SparkSession.builder.getOrCreate()
    do = WorkspaceClient().api_client.do

    # 1. Gold is the chain head's state.
    log = spark.sql(
        "SELECT run_id, baseline_run_id, status, to_version_tag, to_chunks_fingerprint "
        "FROM IDENTIFIER(:table) WHERE source_repo = :repo AND sink = :sink",
        args={"table": config.run_log_table, "repo": config.source_repo, "sink": config.sink},
    ).collect()
    head = pick_baseline([LogEntry(r["run_id"], r["baseline_run_id"], r["status"]) for r in log])
    if head is None:
        raise RuntimeError(f"no committed run for sink {config.sink!r}: nothing to sync")
    head_row = next(r for r in log if r["run_id"] == head)
    rows = spark.sql("SELECT path, chunk_id FROM IDENTIFIER(:table)", args={"table": gold}).collect()
    gold_keys = {r["chunk_id"] for r in rows}
    summary["chain_head"] = {"run_id": head, "tag": head_row["to_version_tag"], "gold_rows": len(rows)}
    if gold_fingerprint([(r["path"], r["chunk_id"]) for r in rows]) != head_row["to_chunks_fingerprint"]:
        raise RuntimeError(f"gold is not the committed state of the chain head {head}: not synced")

    # 2. The index reads gold; its pipeline is idle.
    info = summary_of(index_info(do, config.index))
    summary["index_before"] = info
    if info["source_table"] != gold or info["pipeline_type"] != "TRIGGERED":
        raise RuntimeError(f"{config.index} is not a TRIGGERED Delta Sync index of {gold}: {info}")
    wait_idle(do, info["pipeline_id"])

    # 3. Compare keys; sync only when they differ, then compare again.
    before = compare_keys(scan_keys(do, config.index, "chunk_id"), gold_keys)
    summary["keys_before"] = before
    if before["equal"]:
        summary["sync"] = "skipped: the index already holds exactly gold's keys"
    else:
        summary["sync"] = sync(do, config.index, info["pipeline_id"])
        after = compare_keys(scan_keys(do, config.index, "chunk_id"), gold_keys)
        summary["keys_after"] = after
        if not after["equal"]:
            raise RuntimeError(f"after the sync the index's keys differ from gold's: {after}")
    info = summary_of(index_info(do, config.index))
    summary["index_after"] = info
    if info["indexed_row_count"] != len(gold_keys):
        raise RuntimeError(f"indexed_row_count {info['indexed_row_count']}, gold has {len(gold_keys)} rows")

    # 4. Each probe row finds itself.
    probes = spark.sql(
        f"SELECT chunk_id, embedding FROM IDENTIFIER(:table) ORDER BY chunk_id LIMIT {PROBES}",
        args={"table": gold},
    ).collect()
    results = [
        {
            "chunk_id": p["chunk_id"],
            "top": (
                top := query_keys(do, config.index, "chunk_id", [float(x) for x in p["embedding"]], TOP_K)
            ),
            "found_itself": p["chunk_id"] in top,
        }
        for p in probes
    ]
    summary["probes"] = results
    if not all(r["found_itself"] for r in results):
        raise RuntimeError(f"a probe row is not among its own top {TOP_K}: {results}")


def main() -> None:
    run_with_summary(sync_index, base_parser())


if __name__ == "__main__":
    main()
