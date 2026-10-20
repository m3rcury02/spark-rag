"""Databricks job: detect what a tag changes against the last committed run, gate it, log it (D-011, D-012).

Runs as a serverless spark_python_task (see scripts/run_job.py) for the published chunk config, Config.chunk.
Steps:
1. Check the environment, and that ops_run_log exists (append-only) with the expected columns.
2. Baseline: the head of the chain of committed runs for (source_repo, sink) (docs_rag.changes.pick_baseline).
   With no chain the run needs --initial; a backward or unordered target needs --allow-backwards.
3. Target: the bronze snapshot is whole, and silver holds exactly the chunking of it under Config.chunk: the
   tag is re-chunked here and compared row for row (silver_chunk.py must have run first).
4. Baseline side: bronze still holds the commit and docs the baseline run logged, and silver still holds its
   chunking under the baseline's config, re-chunked and compared the same way, with the chunk fingerprint the
   baseline run logged.
5. Diff docs (by hash) and chunks (by chunk_id), and check that they fit together.
6. The DQ gate (docs_rag.dq, D-012) on the target: every check is appended to ops_dq_results in one insert,
   pass or fail, and read back; a failed check (or an --accept-drift with nothing to accept) stops the run.
   An --accept-drift reason is checked before the Spark session starts.
7. Append the row with status 'committed', which needs the gate's GatePassed: the job's last write, so a
   baseline exists only for a run whose every step passed. A failure after the Spark session started appends
   a 'failed' row instead (never a baseline) and re-raises; a retried attempt of a failed run appends its own
   rows. A retry of a run that already committed writes nothing. This job reads bronze and silver and writes
   only ops_run_log and ops_dq_results.
"""

import argparse
import dataclasses
import datetime as dt
import uuid
from typing import Any

from pyspark.sql import SparkSession

from docs_rag.changes import (
    COMMITTED,
    FAILED,
    RUN_LOG_COLUMNS,
    ChunkRef,
    LogEntry,
    RunLogRow,
    chunk_violations,
    counts,
    diff_chunks,
    diff_docs,
    direction,
    docs_fingerprint,
    pick_baseline,
    row_violations,
    run_log_ddl,
)
from docs_rag.chunk import ChunkConfig
from docs_rag.config import Config, validate_tag
from docs_rag.dq import (
    DQ_RESULTS_COLUMNS,
    DqResult,
    DqRunContext,
    GatePassed,
    Totals,
    dq_results_ddl,
    evaluate,
    gate,
    result_rows,
    validate_override,
)
from docs_rag.jobenv import base_parser, check_environment, run_with_summary
from docs_rag.lake import chunk_bronze, compare_with_stored, read_bronze
from docs_rag.silver import SilverRecord, conflict_reason, fingerprint

JOB = "change_detect"

CREATE_RUN_LOG = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({run_log_ddl()})
COMMENT 'One row per change-detection run; committed rows chain into the baseline history. See D-011.'
TBLPROPERTIES ('delta.appendOnly' = 'true')
"""

CREATE_DQ_RESULTS = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({dq_results_ddl()})
COMMENT 'One row per DQ check per change-detection run attempt, pass or fail. See D-012.'
TBLPROPERTIES ('delta.appendOnly' = 'true')
"""

LOG_ROWS = """
SELECT run_id, baseline_run_id, status, to_version_tag, to_commit_sha, to_config_id, to_docs_fingerprint,
       to_chunks_fingerprint
FROM IDENTIFIER(:table)
WHERE source_repo = :repo AND sink = :sink
"""


def ensure_table(spark: SparkSession, create: str, table: str, columns: tuple[tuple[str, str], ...]) -> None:
    spark.sql(create, args={"table": table})
    found = [
        r["col_name"] for r in spark.sql("DESCRIBE TABLE IDENTIFIER(:table)", args={"table": table}).collect()
    ]
    expected = [name for name, _ in columns]
    if found[: len(expected)] != expected:
        raise RuntimeError(f"{table} columns {found} differ from {expected}: migrate the table first")


def _insert(spark: SparkSession, table: str, row: RunLogRow) -> None:
    problems = row_violations(row)
    if problems:
        raise RuntimeError("run log row fails its checks: " + "; ".join(problems))
    spark.createDataFrame([row.as_row()], run_log_ddl()).createOrReplaceTempView("run_log_incoming")
    spark.sql("INSERT INTO IDENTIFIER(:table) SELECT * FROM run_log_incoming", args={"table": table})


def log_failed(spark: SparkSession, table: str, row: RunLogRow) -> None:
    if row.status != FAILED:
        raise RuntimeError(f"log_failed got a {row.status!r} row")
    _insert(spark, table, row)


def commit(spark: SparkSession, table: str, row: RunLogRow, passed: GatePassed) -> None:
    """Append the committed row: only with the DQ gate's pass, and carrying its override (if any)."""
    if row.status != COMMITTED or row.dq_override != passed.override_reason:
        raise RuntimeError("a committed row must carry the DQ gate's pass and its override reason")
    _insert(spark, table, row)


def write_dq_results(spark: SparkSession, table: str, results: list[DqResult], context: DqRunContext) -> None:
    """All of one attempt's checks in one insert, then read back: the gate decides only on logged results."""
    spark.createDataFrame(result_rows(results, context), dq_results_ddl()).createOrReplaceTempView(
        "dq_results_incoming"
    )
    spark.sql("INSERT INTO IDENTIFIER(:table) SELECT * FROM dq_results_incoming", args={"table": table})
    back = spark.sql(
        "SELECT check_name, status FROM IDENTIFIER(:table) WHERE run_id = :run_id AND attempt_id = :attempt",
        args={"table": table, "run_id": context.run_id, "attempt": context.attempt_id},
    ).collect()
    if sorted((r["check_name"], r["status"]) for r in back) != sorted((r.check, r.status) for r in results):
        raise RuntimeError(f"ops_dq_results read-back differs from the checks: {[tuple(r) for r in back]}")


def refs(records: list[SilverRecord]) -> list[ChunkRef]:
    return [
        ChunkRef(r.path, r.chunk_id, r.n_tokens, (r.chunk_index, r.char_start, r.char_end)) for r in records
    ]


def verified_chunking(
    spark: SparkSession, config: Config, chunking: ChunkConfig, tag: str, run_id: str
) -> tuple[dict[str, str], str, list[SilverRecord]]:
    """Bronze hashes and commit of the tag, and its chunking, after checking that silver holds exactly it."""
    docs = read_bronze(spark, config, tag)
    _, records = chunk_bronze(config, chunking, tag, docs, run_id)
    check, stored = compare_with_stored(spark, config, chunking, tag, records)
    if check.status == "new":
        raise RuntimeError(
            f"silver has no chunking of {tag} under {chunking.config_id}: run silver_chunk first"
        )
    if check.status != "identical":
        raise RuntimeError(
            f"silver's chunking of {tag} under {chunking.config_id} differs from a fresh one: "
            + conflict_reason(docs, stored, config.zero_chunk_docs)
        )
    return {d.path: d.content_sha256 for d in docs}, docs[0].commit_sha, records


def detect(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    started = dt.datetime.now(dt.UTC)
    config = Config(catalog=args.catalog, schema=args.schema)
    chunking = config.chunk
    tag = validate_tag(args.tag)
    accept = validate_override(args.accept_drift, args.initial, config.dq)
    attempt_id = uuid.uuid4().hex
    summary.update(
        {
            "tag": tag,
            "sink": config.sink,
            "config_id": chunking.config_id,
            "accept_drift": accept,
            "attempt_id": attempt_id,
        }
    )
    summary["environment"] = check_environment(args.expect_env, args.expect_version)
    code_version = summary["environment"]["docs_rag"]

    spark = SparkSession.builder.getOrCreate()
    summary["environment"]["spark"] = spark.version
    table = config.run_log_table
    ensure_table(spark, CREATE_RUN_LOG, table, RUN_LOG_COLUMNS)
    ensure_table(spark, CREATE_DQ_RESULTS, config.dq_results_table, DQ_RESULTS_COLUMNS)
    earlier = spark.sql(
        "SELECT status FROM IDENTIFIER(:table) WHERE run_id = :run_id",
        args={"table": table, "run_id": args.run_id},
    ).collect()
    summary["earlier_attempts"] = [r["status"] for r in earlier]
    if any(r["status"] == COMMITTED for r in earlier):  # a retry of a run that already committed: do nothing
        summary["logged"] = "nothing: an earlier attempt of this run already committed"
        return
    known: dict[str, Any] = {}  # what a failed row can still record
    try:
        # 2. The baseline: head of the committed chain for this sink.
        rows = spark.sql(
            LOG_ROWS, args={"table": table, "repo": config.source_repo, "sink": config.sink}
        ).collect()
        head = pick_baseline([LogEntry(r["run_id"], r["baseline_run_id"], r["status"]) for r in rows])
        base = next((r.asDict() for r in rows if r["run_id"] == head), None)
        from_tag = base["to_version_tag"] if base else None
        way = direction(from_tag, tag)
        known.update(baseline_run_id=head, direction=way, from_version_tag=from_tag)
        summary["baseline"] = base
        if base is None and not args.initial:
            raise RuntimeError(
                f"no committed run for sink {config.sink!r}: pass --initial to start the chain"
            )
        if base is not None and args.initial:
            raise RuntimeError(
                f"sink {config.sink!r} already has a chain (head {head}): --initial would fork it"
            )
        if way in ("backward", "unordered") and not args.allow_backwards:
            raise RuntimeError(
                f"{tag} is {way} of the baseline {from_tag}: pass --allow-backwards if intended"
            )

        # 3. The target: bronze whole, silver exactly its chunking under the published config.
        target_docs, target_commit, target_records = verified_chunking(
            spark, config, chunking, tag, args.run_id
        )
        known.update(to_commit_sha=target_commit)

        # 4. The baseline side, pinned by what the baseline run logged.
        base_docs: dict[str, str] | None = None
        base_records: list[SilverRecord] = []
        base_totals: Totals | None = None
        if base is not None:
            from_chunking = next((c for c in config.chunk_grid if c.config_id == base["to_config_id"]), None)
            if from_chunking is None:
                raise RuntimeError(
                    f"the baseline's config {base['to_config_id']} is not in Config.chunk_grid"
                )
            if (base["to_version_tag"], from_chunking) == (tag, chunking):
                base_docs, base_commit, base_records = target_docs, target_commit, target_records
            else:
                base_docs, base_commit, base_records = verified_chunking(
                    spark, config, from_chunking, base["to_version_tag"], args.run_id
                )
            pinned = {
                "commit": (base_commit, base["to_commit_sha"]),
                "docs fingerprint": (docs_fingerprint(base_docs), base["to_docs_fingerprint"]),
                "chunk fingerprint": (
                    fingerprint((r.path, r.chunk_id) for r in base_records),
                    base["to_chunks_fingerprint"],
                ),
            }
            moved = [name for name, (now, logged) in pinned.items() if now != logged]
            if moved:
                raise RuntimeError(f"the baseline {from_tag} no longer matches its logged run: {moved}")
            known.update(from_commit_sha=base_commit, from_config_id=from_chunking.config_id)
            base_totals = Totals(len(base_docs), len(base_records), sum(r.n_tokens for r in base_records))

        # 5. The diffs, and whether they fit together.
        docs = diff_docs(base_docs, target_docs)
        chunks = diff_chunks(refs(base_records), refs(target_records), docs)
        same_config = base is None or base["to_config_id"] == chunking.config_id
        problems = chunk_violations(
            refs(base_records), refs(target_records), docs, config.zero_chunk_docs, same_config
        )
        if problems:
            raise RuntimeError("the chunk diff does not fit the doc diff: " + "; ".join(problems))

        # 6. The DQ gate: log every check, then stop unless they all pass (or drift is accepted).
        results = evaluate(target_records, len(target_docs), base_totals, config, chunking, tag, accept)
        summary["dq"] = [dataclasses.asdict(r) for r in results]
        context = DqRunContext(
            run_id=args.run_id,
            attempt_id=attempt_id,
            source_repo=config.source_repo,
            sink=config.sink,
            config_id=chunking.config_id,
            from_version_tag=from_tag,
            to_version_tag=tag,
            baseline_run_id=head,
            checked_at=dt.datetime.now(dt.UTC),
            code_version=code_version,
        )
        write_dq_results(spark, config.dq_results_table, results, context)
        passed = gate(results, accept)
        summary["gate"] = {"overridden": passed.overridden, "override_reason": passed.override_reason}

        row = RunLogRow(
            run_id=args.run_id,
            baseline_run_id=head,
            job=JOB,
            source_repo=config.source_repo,
            sink=config.sink,
            status=COMMITTED,
            error=None,
            direction=way,
            from_version_tag=from_tag,
            from_commit_sha=known.get("from_commit_sha"),
            from_config_id=known.get("from_config_id"),
            to_version_tag=tag,
            to_commit_sha=target_commit,
            to_config_id=chunking.config_id,
            max_tokens=chunking.max_tokens,
            overlap_tokens=chunking.overlap_tokens,
            to_docs_fingerprint=docs_fingerprint(target_docs),
            to_chunks_fingerprint=fingerprint((r.path, r.chunk_id) for r in target_records),
            started_at=started,
            finished_at=dt.datetime.now(dt.UTC),
            code_version=code_version,
            dq_override=passed.override_reason,
            **counts(docs, chunks, len(base_records), sum(r.n_tokens for r in target_records)),
        )
        summary["row"] = {k: v for k, v in row.as_row().items() if not k.endswith("_paths")}
        summary["paths"] = {"added": docs.added, "changed": docs.changed, "deleted": docs.deleted}

        # 7. Commit: the last write.
        commit(spark, table, row, passed)
    except Exception as e:
        failed = RunLogRow(
            run_id=args.run_id,
            baseline_run_id=known.get("baseline_run_id"),
            job=JOB,
            source_repo=config.source_repo,
            sink=config.sink,
            status=FAILED,
            error=f"{type(e).__name__}: {e}"[:2000],
            direction=known.get("direction"),
            from_version_tag=known.get("from_version_tag"),
            from_commit_sha=known.get("from_commit_sha"),
            from_config_id=known.get("from_config_id"),
            to_version_tag=tag,
            to_commit_sha=known.get("to_commit_sha"),
            to_config_id=chunking.config_id,
            max_tokens=chunking.max_tokens,
            overlap_tokens=chunking.overlap_tokens,
            started_at=started,
            finished_at=dt.datetime.now(dt.UTC),
            code_version=code_version,
        )
        try:
            log_failed(spark, table, failed)
            summary["logged"] = FAILED
        except Exception as log_error:  # noqa: BLE001 - keep the original error; record that logging failed
            summary["logged"] = f"failed row not written: {type(log_error).__name__}: {log_error}"
        raise
    summary["logged"] = COMMITTED

    # Read the row back, and check the chain still ends here (a concurrent run would have forked it).
    back = spark.sql(
        "SELECT * FROM IDENTIFIER(:table) WHERE run_id = :run_id",
        args={"table": table, "run_id": args.run_id},
    ).collect()
    rows_after = spark.sql(
        LOG_ROWS, args={"table": table, "repo": config.source_repo, "sink": config.sink}
    ).collect()
    summary["read_back"] = {
        "rows_with_this_run_id": len(back),
        "status": back[0]["status"] if back else None,
        "chain_head": pick_baseline(
            [LogEntry(r["run_id"], r["baseline_run_id"], r["status"]) for r in rows_after]
        ),
        "committed_runs_in_chain": sum(r["status"] == COMMITTED for r in rows_after),
    }
    if summary["read_back"]["chain_head"] != args.run_id or len(back) != 1:
        raise RuntimeError(f"run log read-back does not end the chain at this run: {summary['read_back']}")


def main() -> None:
    parser = base_parser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--initial", action="store_true", help="start the chain (only when there is none)")
    parser.add_argument(
        "--allow-backwards", action="store_true", help="allow a target older than the baseline"
    )
    parser.add_argument(
        "--accept-drift",
        metavar="REASON",
        help="accept, for this run only, drift over the DQ threshold (not a loss beyond the ceiling); "
        "refused when nothing drifted",
    )
    run_with_summary(detect, parser)


if __name__ == "__main__":
    main()
