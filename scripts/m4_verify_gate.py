"""Check every DQ-gated change-detection run against git, by rules written here, not from docs_rag.dq.

    uv run --with databricks-sdk==0.152.0 python scripts/m4_verify_gate.py --label m4_v_gate_vs_git \
        --job-runs runs/m4_*_detect_*.json --pre-gate-runs runs/m3_*_detect_*.json

The rules are D-012's, written out again below (POLICY). For each job in --job-runs that reached the gate,
the run's ops_dq_results rows must be one attempt (the job's), one row per check, with one config, tags and
baseline that match its run log row and the published config, and the threshold POLICY states; each row's
counts must equal a recount over a local chunking of git's files at the run's tag (for drift, also at its
baseline's tag under the baseline's config); each status must be what POLICY gives for those counts and the
job's --accept-drift. The run must have the outcome the statuses imply (committed exactly when no check failed
and any --accept-drift overrode something; a failed row's error names exactly the failing checks), a committed
row carries the override reason exactly when a check was overridden, and Delta's own history (time travel over
each table's WRITE versions) must show the run's DQ rows written in one insert, before its run log row. A job
that stopped before the gate must have no DQ rows, and a run log row only if it got as far as reading the log.
Across the tables: both are append-only, every DQ row belongs to a logged run, every committed run has a
complete set of DQ rows without a failure except the runs in --pre-gate-runs, and those have no DQ rows and
finished before the first DQ check. Not independent of the pipeline: a chunk's version_tag (the job sets it;
git chunks have none) and the chunker itself (verified in M2).
"""

import argparse
import datetime as dt
import json
import pathlib
import re
import subprocess
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import TagDocs, read_tags
from redact import Redactor
from run_sql import run_statement
from runguard import REPO, committed_head

from docs_rag.chunk import Chunk, ChunkConfig, chunk_document
from docs_rag.config import Config
from docs_rag.tokens import count_tokens

POLICY = {
    "tiny_body_tokens": 5,
    "empty_chunks": 0.0,
    "tiny_chunks": 0.02,
    "oversized_chunks": 0.0,
    "duplicate_chunk_ids": 0.0,
    "duplicate_embed_text": 0.01,
    "missing_metadata": 0.0,
    "drift": 0.10,
    "max_override_loss": 0.50,
    "model_input_tokens": 256 - 2,  # all-MiniLM-L6-v2's 256 less [CLS] and [SEP]
}
# The first commit whose change_detect job runs the gate: a run whose code descends from it is gated.
GATE_COMMIT = "41300433db257a33a29934b129b4cbcf54e5d272"
DRIFT = ("drift_docs", "drift_chunks", "drift_tokens")
COUNT_CHECKS = (
    "empty_chunks",
    "tiny_chunks",
    "oversized_chunks",
    "duplicate_chunk_ids",
    "duplicate_embed_text",
    "missing_metadata",
)


def chunks_of(tag: TagDocs, config: ChunkConfig) -> list[Chunk]:
    return [c for p, text in sorted(tag.files.items()) for c in chunk_document(p, text, config).chunks]


def recount(chunks: list[Chunk], tag: TagDocs, config: ChunkConfig) -> dict[str, int]:
    """Rows breaking each count rule, by plain loops."""
    limit = min(config.max_tokens, POLICY["model_input_tokens"])
    ids = [c.chunk_id for c in chunks]
    texts = [c.embed_text for c in chunks]
    bad_meta = 0
    for c in chunks:
        ok_path = c.path.startswith("docs/") and c.path.endswith(".md")
        ok_section = len(c.section_path) > 0 and all(s.strip() != "" for s in c.section_path)
        ok_commit = re.fullmatch(r"[0-9a-f]{40}", tag.commit_sha) is not None
        bad_meta += not (ok_path and ok_section and ok_commit)
    return {
        "empty_chunks": sum(
            count_tokens(c.body) == 0 or c.body.strip() == "" or c.embed_text.strip() == "" for c in chunks
        ),
        "tiny_chunks": sum(count_tokens(c.body) < POLICY["tiny_body_tokens"] for c in chunks),
        "oversized_chunks": sum(max(count_tokens(c.embed_text), c.n_tokens) > limit for c in chunks),
        "duplicate_chunk_ids": len(ids) - len(set(ids)),
        "duplicate_embed_text": len(texts) - len(set(texts)),
        "missing_metadata": bad_meta,
    }


def as_number(value: Any) -> Any:
    """The statement API returns every value as a string."""
    if value is None:
        return None
    return float(value) if any(ch in value for ch in ".eE") else int(value)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--job-runs", nargs="+", required=True, help="runs/ files of DQ-gated change_detect jobs")
    ap.add_argument("--pre-gate-runs", nargs="*", default=[], help="runs/ files of runs before the gate")
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    config = Config()
    grid = {c.config_id: c for c in config.chunk_grid}
    w = WorkspaceClient()
    warehouse = next(iter(w.warehouses.list())).id or ""

    def query(sql: str, params: dict[str, str]) -> list[dict[str, Any]]:
        result = run_statement(w, warehouse, sql, params)
        return [dict(zip(result["columns"], r, strict=True)) for r in result["rows"]]

    log = query(
        "SELECT * FROM IDENTIFIER(:table) WHERE source_repo = :repo AND sink = :sink",
        {"table": config.run_log_table, "repo": config.source_repo, "sink": config.sink},
    )
    dq = query("SELECT * FROM IDENTIFIER(:table)", {"table": config.dq_results_table})
    props = {
        t: {r["key"]: r["value"] for r in query("SHOW TBLPROPERTIES IDENTIFIER(:table)", {"table": t})}
        for t in (config.run_log_table, config.dq_results_table)
    }
    checks: list[dict[str, Any]] = []

    def add(scope: str, name: str, want: Any, got: Any, ok: bool | None = None) -> None:
        checks.append(
            {
                "scope": scope,
                "check": name,
                "expected": want,
                "logged": got,
                "pass": want == got if ok is None else ok,
            }
        )

    for table, p in props.items():
        add(table, "delta.appendOnly", "true", p.get("delta.appendOnly"))

    by_run: dict[str, list[dict[str, Any]]] = {}
    for r in dq:
        by_run.setdefault(r["run_id"], []).append(r)
    log_by_run: dict[str, list[dict[str, Any]]] = {}
    for r in log:
        log_by_run.setdefault(r["run_id"], []).append(r)
    committed = {r["run_id"]: r for r in log if r["status"] == "committed"}

    def ts(value: str) -> dt.datetime:
        t = dt.datetime.fromisoformat(value)
        return t if t.tzinfo else t.replace(tzinfo=dt.UTC)

    def first_writes(table: str) -> dict[str, tuple[int, dt.datetime, int]]:
        """run_id -> (the WRITE version its rows first appear in, that commit's time, its rows there), read
        from Delta's own history by time travel, not from any timestamp the job wrote."""
        history = query("DESCRIBE HISTORY IDENTIFIER(:table)", {"table": table})
        first: dict[str, tuple[int, dt.datetime, int]] = {}
        for h in sorted(history, key=lambda h: int(h["version"])):
            if h["operation"] != "WRITE":
                continue
            v = int(h["version"])  # table names come from Config (validated identifiers), v is an int
            for r in query(
                f"SELECT run_id, count(*) AS n FROM {table} VERSION AS OF {v} GROUP BY run_id", {}
            ):
                first.setdefault(r["run_id"], (v, ts(h["timestamp"]), int(r["n"])))
        return first

    dq_first = first_writes(config.dq_results_table)
    log_first = first_writes(config.run_log_table)
    all_checks = sorted([*COUNT_CHECKS, *DRIFT])

    jobs = []
    for f in args.job_runs:
        data = json.loads(pathlib.Path(f).read_text())
        jobs.append((pathlib.Path(f).name, data.get("summary") or {}))
    tags_needed = set()
    for _, s in jobs:
        for r in by_run.get(s.get("run_id", ""), []):
            tags_needed.add(r["to_version_tag"])
            if r["from_version_tag"]:
                tags_needed.add(r["from_version_tag"])
    source = read_tags(sorted(tags_needed))
    chunk_cache: dict[tuple[str, str], list[Chunk]] = {}

    def chunked(tag: str, config_id: str) -> list[Chunk]:
        key = (tag, config_id)
        if key not in chunk_cache:
            chunk_cache[key] = chunks_of(source[tag], grid[config_id])
        return chunk_cache[key]

    for name, s in jobs:
        run_id = s.get("run_id") or ""
        scope = f"job {name}"
        if not run_id:  # a refused submit, or a job that died before writing its summary
            add(scope, "job summary has a run_id (verifiable)", True, False)
            continue
        rows = by_run.get(run_id, [])
        logged = log_by_run.get(run_id, [])
        if "dq" not in s:  # the job stopped before the gate evaluated anything
            add(scope, "no DQ rows (stopped before the gate)", 0, len(rows))
            add(scope, "job failed", "failed", s.get("status"))
            if s.get("logged") != "failed":  # it stopped before its failed row could be written (D-011)
                add(scope, "no run log row (stopped before logging could start)", 0, len(logged))
            else:
                add(scope, "one failed run log row", ["failed"], [r["status"] for r in logged])
                add(
                    scope,
                    "its error is not the gate's",
                    False,
                    str(s.get("error", "")).startswith("DqFailed"),
                )
            continue

        add(
            scope,
            "DQ rows come from one attempt, the job's",
            [s.get("attempt_id")],
            sorted({r["attempt_id"] for r in rows}),
        )
        add(scope, "one DQ row per check", all_checks, sorted(r["check_name"] for r in rows))
        keys = {
            (r["config_id"], r["from_version_tag"], r["to_version_tag"], r["baseline_run_id"]) for r in rows
        }
        add(scope, "DQ rows share one config, tags and baseline", 1, len(keys))
        if not rows or len(keys) != 1:
            continue
        config_id, from_tag, tag, baseline_id = next(iter(keys))
        add(
            scope,
            "DQ config = the published config = the job's",
            [config.chunk.config_id] * 2,
            [config_id, s.get("config_id")],
        )
        add(scope, "DQ rows name the job's tag", s.get("tag"), tag)
        add(scope, "one run log row", 1, len(logged))
        if logged:
            row = logged[0]
            add(
                scope,
                "DQ rows and run log row agree on config, tags and baseline",
                (row["to_config_id"], row["from_version_tag"], row["to_version_tag"], row["baseline_run_id"]),
                (config_id, from_tag, tag, baseline_id),
            )
        if config_id not in grid:
            add(scope, "config is in Config.chunk_grid", True, False)
            continue
        target = chunked(tag, config_id)
        bad = recount(target, source[tag], grid[config_id])
        accept = s.get("accept_drift")
        baseline_row = committed.get(baseline_id or "")
        if from_tag:
            add(
                scope,
                "the baseline is a committed run of the from tag",
                from_tag,
                (baseline_row or {}).get("to_version_tag"),
            )
        base_chunks = (
            chunked(from_tag, baseline_row["to_config_id"])
            if from_tag and baseline_row and baseline_row["to_config_id"] in grid
            else None
        )
        totals_to = (len(source[tag].files), len(target), sum(c.n_tokens for c in target))
        totals_from = (
            (len(source[from_tag].files), len(base_chunks), sum(c.n_tokens for c in base_chunks))
            if from_tag and base_chunks is not None
            else None
        )
        expected_status: dict[str, str] = {}
        for r in rows:
            check = r["check_name"]
            got = {k: as_number(r[k]) for k in ("numerator", "denominator", "observed", "threshold")}
            if check in COUNT_CHECKS:
                n = len(target)
                want_num, want_den, limit = bad[check], n, POLICY[check]
                status = "pass" if want_num <= limit * want_den else "fail"
                observed: float | None = want_num / want_den
            else:
                i = DRIFT.index(check)
                want_num, limit = totals_to[i], POLICY["drift"]
                if totals_from is None:
                    want_den, observed, status = 0, None, "not_applicable"
                else:
                    want_den = totals_from[i]
                    observed = want_num / want_den - 1
                    if abs(want_num - want_den) <= limit * want_den:
                        status = "pass"
                    elif accept and want_den - want_num <= POLICY["max_override_loss"] * want_den:
                        status = "overridden"
                    else:
                        status = "fail"
            expected_status[check] = status
            add(scope, f"{check} numerator", want_num, got["numerator"])
            add(scope, f"{check} denominator", want_den, got["denominator"])
            add(scope, f"{check} threshold = POLICY", limit, got["threshold"])
            add(
                scope,
                f"{check} observed",
                observed,
                got["observed"],
                ok=(observed is None and got["observed"] is None)
                or (
                    observed is not None
                    and got["observed"] is not None
                    and abs(observed - got["observed"]) < 1e-9
                ),
            )
            add(scope, f"{check} status", status, r["status"])
            add(
                scope,
                f"{check} override reason",
                accept if status == "overridden" else None,
                r["override_reason"],
            )

        failing = sorted(c for c, st in expected_status.items() if st == "fail")
        overridden = [c for c, st in expected_status.items() if st == "overridden"]
        should_commit = not failing and (accept is None or bool(overridden))
        add(
            scope,
            "job outcome follows the checks",
            "succeeded" if should_commit else "failed",
            s.get("status"),
        )
        if logged:
            row = logged[0]
            add(
                scope,
                "run outcome follows the checks",
                "committed" if should_commit else "failed",
                row["status"],
            )
            add(
                scope,
                "dq_override = the reason iff the run committed with an override",
                accept if overridden and should_commit else None,
                row["dq_override"],
            )
            if not should_commit:
                error = str(row["error"])
                named = sorted(c for c in all_checks if f"{c} fail" in error)
                add(scope, "the failed row's error is the gate's", True, error.startswith("DqFailed"))
                add(scope, "the error names exactly the failing checks", failing, named)
                if not failing:
                    add(scope, "an unused --accept-drift is refused", True, "nothing to accept" in error)
        # Write-audit-publish, from Delta's history: the checks went in one insert, before the run's row.
        dq_write, log_write = dq_first.get(run_id), log_first.get(run_id)
        add(scope, "all DQ rows written in one insert", len(all_checks), dq_write[2] if dq_write else None)
        add(scope, "run log row written once", 1, log_write[2] if log_write else None)
        add(
            scope,
            "DQ rows written before the run log row",
            True,
            bool(dq_write and log_write and dq_write[1] < log_write[1]),
        )

    # Across the tables.
    pre_gate = set()
    for f in args.pre_gate_runs:
        data = json.loads(pathlib.Path(f).read_text())
        summary = data.get("summary") or {}
        if not summary.get("run_id"):
            continue
        pre_gate.add(summary["run_id"])
        commit = (data.get("meta") or {}).get("git_commit", "")
        gated = subprocess.run(
            ["git", "-C", str(REPO), "merge-base", "--is-ancestor", GATE_COMMIT, commit], capture_output=True
        ).returncode  # 0: commit descends from the gate commit; 1: it does not; else: unknown commit
        add(f"pre-gate {pathlib.Path(f).name}", "its code predates the gate", 1, gated)
    add("tables", "DQ rows without a run log row", [], sorted(set(by_run) - set(log_by_run)))
    ungated = []
    for run_id in committed:
        if run_id in pre_gate:
            continue
        rows = by_run.get(run_id, [])
        complete = sorted(r["check_name"] for r in rows) == all_checks
        if not complete or any(r["status"] == "fail" for r in rows):
            ungated.append(run_id)
    add("tables", "committed runs since the gate without a complete passing DQ set", [], ungated)
    add("tables", "pre-gate runs with DQ rows", [], sorted(pre_gate & set(by_run)))
    first_check = min((ts(r["checked_at"]) for r in dq), default=None)
    late = sorted(
        r
        for r in pre_gate & set(committed)
        if first_check is None or ts(committed[r]["finished_at"]) >= first_check
    )
    add("tables", "pre-gate committed runs that finished after the first DQ check", [], late)

    out = {
        "label": args.label,
        "git_commit": head,
        "policy": POLICY,
        "dq_rows": len(dq),
        "pre_gate_runs_exempted": sorted(pre_gate & set(committed)),
        "log_rows": len(log),
        "checks": checks,
        "all_pass": all(c["pass"] for c in checks),
    }
    out_path.write_text(Redactor(w).dump(out))
    for c in checks:
        print("PASS" if c["pass"] else "FAIL", c["scope"], "|", c["check"])
    print("all_pass:", out["all_pass"], f"({sum(c['pass'] for c in checks)}/{len(checks)})")


if __name__ == "__main__":
    main()
