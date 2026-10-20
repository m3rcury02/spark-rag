"""Databricks job: detect a tag's changes against the last committed run, gate, log (D-011, D-012, D-013).

Runs as a serverless spark_python_task (see scripts/run_job.py) for the published chunk config, Config.chunk.
Steps:
1. Check the environment, and that ops_run_log exists (append-only) with the expected columns.
2. Baseline: the head of the chain of committed runs for (source_repo, sink) (docs_rag.changes.pick_baseline).
   With no chain the run needs --initial; a backward or unordered target needs --allow-backwards.
3. Target: the bronze snapshot is whole, and silver holds exactly the chunking of it under Config.chunk: the
   tag is re-chunked here and compared row for row (silver_chunk.py must have run first).
4. Baseline side: bronze still holds the commit and docs the baseline run logged, and silver still holds its
   chunking under the baseline's config with the chunk fingerprint the baseline run logged: re-chunked and
   compared the same way when this code can reproduce that config, else checked from the stored rows (D-013's
   re-baseline path; a cleaner or chunker version bump). A target under another config than the baseline's
   needs --allow-config-change, and the flag is refused when the config does not change.
5. Diff docs (by hash) and chunks (by chunk_id), and check that they fit together.
6. The DQ gate (docs_rag.dq, D-012) on the target: every check is appended to ops_dq_results in one insert,
   pass or fail, and read back; a failed check (or an accepted drift that is not there) stops the run. Drift
   over its threshold passes only with --accept-drift "<reason>" --accept-drift-of <run_id>, naming a run that
   failed the gate on exactly this drift (checked right after the baseline is known); the pair is checked for
   form before the Spark session starts.
With a sink (Config.sink, the gold table; D-014), three more steps publish the target:
7. Gold state: gold holds exactly the chain head's chunks (its logged chunk fingerprint; nothing for an
   initial run), all of the published model. Gold holding this run's target instead means an earlier attempt
   merged but never committed: the run stops, and gold is repaired by hand (no automatic resume, D-014).
8. Embed only the added chunks (docs_rag.embedder: the pinned model, CPU, normalized), plus the canary texts.
   The vector checks (docs_rag.dq.vector_checks) go to ops_dq_results as the attempt's second insert, and
   stop the run unless they all pass. Nothing added: no model, no vector rows.
9. One MERGE into gold: insert the added chunks, delete the removed ones, never update; it needs both gates'
   passes. Its own metrics and the table history's must equal the chunk diff, and afterwards gold must hold
   exactly the target's chunks. No change: no MERGE (a no-op MERGE still writes a table version, D-006).
   The AI Search index is synced from gold by jobs/sync_index.py, after the commit: the index is a replica.
10. (7 without a sink) Append the row with status 'committed', which needs the gate's GatePassed: the job's
   last write, so a baseline exists only for a run whose every step passed. A failure after the Spark session
   started appends a 'failed' row instead (never a baseline; it records the gold version if the MERGE ran) and
   re-raises; a retried attempt of a failed run appends its own rows. A retry of a run that already committed
   writes nothing. This job reads bronze and silver and writes ops_run_log, ops_dq_results and gold.
"""

import argparse
import dataclasses
import datetime as dt
import time
import uuid
from collections.abc import Sequence
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
    PASS,
    Acceptance,
    Baseline,
    DqFailed,
    DqResult,
    DqRunContext,
    GatePassed,
    LoggedCheck,
    Totals,
    VectorsPassed,
    accept_failed_run,
    canary_check,
    describe,
    dq_results_ddl,
    evaluate,
    gate,
    gate_vectors,
    result_rows,
    validate_override,
    vector_checks,
)
from docs_rag.embedder import canary_cosines, encode, load_canary, load_model
from docs_rag.gold import (
    GOLD_COLUMNS,
    MERGE_GOLD,
    GoldRow,
    check_gold_state,
    check_merge_counts,
    gold_ddl,
    gold_fingerprint,
    gold_rows,
    history_merge_metrics,
    merge_source_ddl,
    merge_source_rows,
    merge_version,
)
from docs_rag.jobenv import base_parser, check_environment, check_packages, run_with_summary
from docs_rag.lake import chunk_bronze, compare_with_stored, latest_version, read_bronze, stored_baseline
from docs_rag.silver import SilverRecord, StoredChunk, conflict_reason, fingerprint

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

CREATE_GOLD = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({gold_ddl()})
COMMENT 'One row per published chunk and its embedding: the source of the AI Search index. See D-014.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
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
    if row.status != COMMITTED or (row.dq_override, row.dq_override_of) != (
        passed.override_reason,
        passed.override_of,
    ):
        raise RuntimeError("a committed row must carry the DQ gate's pass and its override")
    _insert(spark, table, row)


def write_dq_results(spark: SparkSession, table: str, results: list[DqResult], context: DqRunContext) -> None:
    """One gate's checks in one insert (an attempt has two with a sink: chunk checks, then vector checks),
    then read back: the gate decides only on logged results."""
    spark.createDataFrame(result_rows(results, context), dq_results_ddl()).createOrReplaceTempView(
        "dq_results_incoming"
    )
    spark.sql("INSERT INTO IDENTIFIER(:table) SELECT * FROM dq_results_incoming", args={"table": table})
    back = spark.sql(
        "SELECT check_name, status FROM IDENTIFIER(:table) WHERE run_id = :run_id AND attempt_id = :attempt",
        args={"table": table, "run_id": context.run_id, "attempt": context.attempt_id},
    ).collect()
    names = {r.check for r in results}
    got = sorted((r["check_name"], r["status"]) for r in back if r["check_name"] in names)
    if got != sorted((r.check, r.status) for r in results):
        raise RuntimeError(f"ops_dq_results read-back differs from the checks: {got}")


def ensure_gold(spark: SparkSession, table: str) -> None:
    ensure_table(spark, CREATE_GOLD, table, GOLD_COLUMNS)
    # DESCRIBE DETAIL takes no parameter; the name is built from identifiers Config validated.
    properties = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]["properties"] or {}
    if properties.get("delta.enableChangeDataFeed") != "true":  # the Delta Sync index reads the change feed
        raise RuntimeError(f"{table} has no change data feed: {dict(properties)}")


def read_gold(spark: SparkSession, table: str) -> dict[str, Any]:
    """What gold holds: its (path, chunk_id) fingerprint, row count and models."""
    rows = spark.sql(
        "SELECT path, chunk_id, embedding_model FROM IDENTIFIER(:table)", args={"table": table}
    ).collect()
    return {
        "fingerprint": gold_fingerprint([(r["path"], r["chunk_id"]) for r in rows]),
        "rows": len(rows),
        "models": sorted({r["embedding_model"] for r in rows}),
    }


def merge_gold(
    spark: SparkSession,
    table: str,
    inserts: list[GoldRow],
    deletes: Sequence[str],
    passed: GatePassed,
    vectors: VectorsPassed,
    known: dict[str, Any],
) -> dict[str, Any]:
    """The one write to gold: needs the DQ gate's pass and the vector gate's pass for exactly these rows. Once
    the MERGE statement returns or raises, the history is read and a MERGE version found there goes into
    `known` before anything is checked, so a failed row still points the repair at it (D-014)."""
    if not isinstance(passed, GatePassed) or vectors.checked != len(inserts):
        raise RuntimeError("the gold MERGE needs the DQ gate's pass and vector checks of exactly its rows")
    before = latest_version(spark, table)
    spark.createDataFrame(merge_source_rows(inserts, deletes), merge_source_ddl()).createOrReplaceTempView(
        "gold_changes"
    )
    try:
        merged = spark.sql(MERGE_GOLD, args={"table": table}).collect()[0].asDict()
    finally:
        history = (
            spark.sql("DESCRIBE HISTORY IDENTIFIER(:table)", args={"table": table})
            .where(f"version > {before}")
            .collect()
        )
        merges = [int(r["version"]) for r in history if r["operation"] == "MERGE"]
        known.update(gold_version=merges[0] if len(merges) == 1 else None)
    metrics = {k: int(v) for k, v in merged.items()}
    version = merge_version([(int(r["version"]), r["operation"]) for r in history], before)
    logged = next(r["operationMetrics"] for r in history if int(r["version"]) == version)
    check_merge_counts(metrics, len(inserts), len(deletes))
    check_merge_counts(history_merge_metrics(logged), len(inserts), len(deletes))
    known.update(
        gold_rows_inserted=metrics["num_inserted_rows"], gold_rows_deleted=metrics["num_deleted_rows"]
    )
    return {
        "version_before": before,
        "version": version,
        "merge_metrics": metrics,
        "history": [
            {"version": r["version"], "operation": r["operation"], "metrics": r["operationMetrics"]}
            for r in history
        ],
    }


def publish(
    spark: SparkSession,
    config: Config,
    table: str,
    run_id: str,
    base: dict[str, Any] | None,
    target_records: Sequence[SilverRecord],
    added: Sequence[str],
    removed: Sequence[str],
    passed: GatePassed,
    context: DqRunContext,
    summary: dict[str, Any],
    known: dict[str, Any],
) -> None:
    """Steps 7-9: gold state, embed the added chunks and check the vectors, MERGE, check gold after. Fills
    `known` with the run-log fields of what it published."""
    embedding = config.embedding
    target = fingerprint((r.path, r.chunk_id) for r in target_records)
    # 7. Gold holds the chain head's chunks, all of the published model.
    before = read_gold(spark, table)
    summary["gold_before"] = before
    if set(before["models"]) - {embedding.model_id}:
        raise RuntimeError(
            f"gold holds vectors of {before['models']}, the published model is {embedding.model_id}"
        )
    check_gold_state(before["fingerprint"], base["to_chunks_fingerprint"] if base else None, target)

    # 8. Embed only what was added; the vector checks are logged, then gate.
    wanted = set(added)
    to_embed = sorted((r for r in target_records if r.chunk_id in wanted), key=lambda r: r.chunk_id)
    vectors: list[list[float]] = []
    results: list[DqResult] = []
    if to_embed:
        t0 = time.monotonic()
        model = load_model(embedding)
        canary = load_canary()
        cosines = canary_cosines(canary, embedding, encode(model, canary.texts, embedding))
        summary["canary_cosines"] = cosines
        canary_result = canary_check(cosines, config.dq)
        if canary_result.status != PASS:  # a wrong model: log the one row and stop before the long encode
            checked = dataclasses.replace(context, checked_at=dt.datetime.now(dt.UTC))
            write_dq_results(spark, config.dq_results_table, [canary_result], checked)
            raise DqFailed("vector checks failed: " + describe(canary_result))
        t1 = time.monotonic()
        vectors = encode(model, [r.embed_text for r in to_embed], embedding)
        t2 = time.monotonic()
        summary["embedding"] = {
            "model_id": embedding.model_id,
            "chunks": len(to_embed),
            "tokens": sum(r.n_tokens for r in to_embed),
            "model_load_seconds": round(t1 - t0, 1),
            "encode_seconds": round(t2 - t1, 1),
            "chunks_per_second": round(len(to_embed) / max(t2 - t1, 1e-9), 1),
            "canary_cosines": cosines,
        }
        results = vector_checks(
            [r.chunk_id for r in to_embed], frozenset(added), vectors, embedding.dimension, cosines, config.dq
        )
        summary["vector_dq"] = [dataclasses.asdict(r) for r in results]
        checked = dataclasses.replace(context, checked_at=dt.datetime.now(dt.UTC))
        write_dq_results(spark, config.dq_results_table, results, checked)
    known.update(
        embedding_model=embedding.model_id,
        chunks_embedded=len(to_embed),
        embedded_ids_fingerprint=fingerprint((r.path, r.chunk_id) for r in to_embed),
    )
    vectors_passed = gate_vectors(results, len(added))

    # 9. One MERGE (none when nothing changed), then gold must hold exactly the target.
    if not added and not removed:
        summary["gold_merge"] = "skipped: nothing added or removed"
        known.update(gold_rows_inserted=0, gold_rows_deleted=0, gold_version=None)
    else:
        inserts = gold_rows(to_embed, vectors, embedding.model_id, run_id, dt.datetime.now(dt.UTC))
        summary["gold_merge"] = merge_gold(spark, table, inserts, removed, passed, vectors_passed, known)
    after = read_gold(spark, table)
    by_run = spark.sql(
        "SELECT count(*) AS n FROM IDENTIFIER(:table) WHERE embedded_by_run_id = :run_id",
        args={"table": table, "run_id": run_id},
    ).collect()[0]["n"]
    summary["gold_after"] = {**after, "rows_of_this_run": by_run}
    if (
        after["fingerprint"] != target
        or by_run != len(to_embed)
        or set(after["models"]) - {embedding.model_id}
    ):
        raise RuntimeError(f"gold after the MERGE is not the target: {summary['gold_after']}")


def read_acceptance(
    spark: SparkSession,
    config: Config,
    override: tuple[str, str],
    tag: str,
    from_tag: str | None,
    baseline_run_id: str | None,
) -> Acceptance:
    """The --accept-drift-of run's run-log statuses and DQ rows, bound to this run (docs_rag.dq)."""
    reason, run_id = override
    statuses = [
        r["status"]
        for r in spark.sql(
            "SELECT status FROM IDENTIFIER(:table) "
            "WHERE run_id = :run_id AND source_repo = :repo AND sink = :sink",
            args={
                "table": config.run_log_table,
                "run_id": run_id,
                "repo": config.source_repo,
                "sink": config.sink,
            },
        ).collect()
    ]
    rows = [
        LoggedCheck(
            r["run_id"],
            r["attempt_id"],
            r["check_name"],
            r["status"],
            int(r["numerator"]),
            int(r["denominator"]),
            r["config_id"],
            r["from_version_tag"],
            r["to_version_tag"],
            r["baseline_run_id"],
        )
        for r in spark.sql(
            "SELECT * FROM IDENTIFIER(:table) "
            "WHERE run_id = :run_id AND source_repo = :repo AND sink = :sink",
            args={
                "table": config.dq_results_table,
                "run_id": run_id,
                "repo": config.source_repo,
                "sink": config.sink,
            },
        ).collect()
    ]
    return accept_failed_run(
        reason,
        run_id,
        statuses,
        rows,
        config_id=config.chunk.config_id,
        from_tag=from_tag,
        tag=tag,
        baseline_run_id=baseline_run_id,
    )


def refs(records: Sequence[SilverRecord | StoredChunk]) -> list[ChunkRef]:
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
    override = validate_override(args.accept_drift, args.accept_drift_of, args.initial, config.dq)
    attempt_id = uuid.uuid4().hex
    summary.update(
        {
            "tag": tag,
            "sink": config.sink,
            "config_id": chunking.config_id,
            "accept_drift": override[0] if override else None,
            "accept_drift_of": override[1] if override else None,
            "attempt_id": attempt_id,
        }
    )
    summary["environment"] = check_environment(args.expect_env, args.expect_version)
    code_version = summary["environment"]["docs_rag"]
    gold = config.gold_table
    if gold is not None:  # the job's pins for embedding (scripts/run_job.py installs them)
        summary["environment"]["embedding_packages"] = check_packages(config.embedding.packages)

    spark = SparkSession.builder.getOrCreate()
    summary["environment"]["spark"] = spark.version
    table = config.run_log_table
    ensure_table(spark, CREATE_RUN_LOG, table, RUN_LOG_COLUMNS)
    ensure_table(spark, CREATE_DQ_RESULTS, config.dq_results_table, DQ_RESULTS_COLUMNS)
    if gold is not None:
        ensure_gold(spark, gold)
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
        config_change = base is not None and base["to_config_id"] != chunking.config_id
        if config_change and not args.allow_config_change:
            raise RuntimeError(
                f"the baseline was chunked under config {base['to_config_id']}, this run uses "
                f"{chunking.config_id}: pass --allow-config-change if intended"
            )
        if (
            args.allow_config_change and not config_change
        ):  # a standing flag must not wave future changes through
            raise RuntimeError(
                "--allow-config-change was given, but the config does not change: nothing to allow"
            )
        accept = None
        if override:  # bind the override to the failed run it names, before the expensive steps
            accept = read_acceptance(spark, config, override, tag, from_tag, head)
            summary["acceptance"] = {"run_id": accept.run_id, "drift": sorted(accept.drift)}

        # 3. The target: bronze whole, silver exactly its chunking under the published config.
        target_docs, target_commit, target_records = verified_chunking(
            spark, config, chunking, tag, args.run_id
        )
        known.update(to_commit_sha=target_commit)

        # 4. The baseline side, pinned by what the baseline run logged. A config this code can re-chunk is
        # re-chunked and compared with silver (D-011); one it cannot (an earlier cleaner or chunker version)
        # is checked from its stored silver rows (D-013's re-baseline path). Either way the pins below hold.
        base_docs: dict[str, str] | None = None
        base_records: Sequence[SilverRecord | StoredChunk] = []
        baseline: Baseline | None = None
        if base is not None:
            from_chunking = next((c for c in config.chunk_grid if c.config_id == base["to_config_id"]), None)
            check = "rechunked" if from_chunking is not None else "stored"
            known.update(baseline_check=check)
            summary["baseline_check"] = check
            if from_chunking is None:
                bronze, base_records = stored_baseline(
                    spark, config, base["to_version_tag"], base["to_config_id"]
                )
                base_docs = {d.path: d.content_sha256 for d in bronze}
                base_commit = bronze[0].commit_sha
            elif (base["to_version_tag"], from_chunking) == (tag, chunking):
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
            known.update(from_commit_sha=base_commit, from_config_id=base["to_config_id"])
            baseline = Baseline(
                base["to_version_tag"],
                Totals(len(base_docs), len(base_records), sum(r.n_tokens for r in base_records)),
            )

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
        results = evaluate(target_records, len(target_docs), baseline, config, chunking, tag, accept)
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
        summary["gate"] = {
            "overridden": passed.overridden,
            "override_reason": passed.override_reason,
            "override_of": passed.override_of,
        }

        # 7-9. Publish to gold: embed the added chunks, check the vectors, MERGE.
        if gold is not None:
            publish(
                spark,
                config,
                gold,
                args.run_id,
                base,
                target_records,
                chunks.added,
                chunks.removed,
                passed,
                context,
                summary,
                known,
            )

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
            dq_override_of=passed.override_of,
            baseline_check=known.get("baseline_check"),
            embedding_model=known.get("embedding_model"),
            chunks_embedded=known.get("chunks_embedded"),
            embedded_ids_fingerprint=known.get("embedded_ids_fingerprint"),
            gold_rows_inserted=known.get("gold_rows_inserted"),
            gold_rows_deleted=known.get("gold_rows_deleted"),
            gold_version=known.get("gold_version"),
            **counts(docs, chunks, len(base_records), sum(r.n_tokens for r in target_records)),
        )
        summary["row"] = {k: v for k, v in row.as_row().items() if not k.endswith("_paths")}
        summary["paths"] = {"added": docs.added, "changed": docs.changed, "deleted": docs.deleted}

        # 10. Commit: the last write.
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
            baseline_check=known.get("baseline_check"),
            embedding_model=known.get("embedding_model"),
            chunks_embedded=known.get("chunks_embedded"),
            embedded_ids_fingerprint=known.get("embedded_ids_fingerprint"),
            gold_rows_inserted=known.get("gold_rows_inserted"),
            gold_rows_deleted=known.get("gold_rows_deleted"),
            gold_version=known.get("gold_version"),
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
        "--allow-config-change",
        action="store_true",
        help="allow a target chunked under another config than the baseline (e.g. a cleaner version bump)",
    )
    parser.add_argument(
        "--accept-drift",
        metavar="REASON",
        help="why the drift of the run named by --accept-drift-of is accepted, for this run only",
    )
    parser.add_argument(
        "--accept-drift-of",
        metavar="RUN_ID",
        help="a run that failed the gate on drift alone, for the same tag, baseline and config; this run "
        "passes only if its drift is exactly that run's (never a loss beyond the ceiling)",
    )
    run_with_summary(detect, parser)


if __name__ == "__main__":
    main()
