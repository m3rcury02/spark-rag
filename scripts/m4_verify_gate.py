"""Check every DQ-gated change-detection run against git, by rules written here, not from docs_rag.dq.

    uv run --with databricks-sdk==0.152.0 python scripts/m4_verify_gate.py --label m4_v_gate_vs_git \
        --job-runs runs/m4_*_detect_*.json --pre-gate-runs runs/m3_*_detect_*.json

The rules are D-012's, written out again below (POLICY). For each job in --job-runs, the run's ops_dq_results
rows must be one attempt (the job's), one row per check, with the threshold POLICY states; each row's counts
must equal a recount over a local chunking of git's files at the run's tag (for drift, also at its baseline's
tag under the baseline's config); each status must be what POLICY gives for those counts and the job's
--accept-drift. The run must have the outcome the statuses imply: committed exactly when no check failed and
any --accept-drift overrode something. A committed row carries the override reason exactly when a check was
overridden, and its checks were logged before it. Across the tables: both are append-only, every DQ row
belongs to a logged run, every committed run has a complete set of DQ rows without a failure except the runs
in --pre-gate-runs (from before the gate), and those have no DQ rows. Not independent of the pipeline: the
version_tag of a chunk (the job sets it; git chunks have none) and the chunker itself (verified in M2).
"""

import argparse
import datetime as dt
import json
import pathlib
import re
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
        "empty_chunks": sum(c.body.strip() == "" or c.embed_text.strip() == "" for c in chunks),
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
        run_id = s.get("run_id")
        scope = f"job {name}"
        rows = by_run.get(run_id or "", [])
        attempts = sorted({r["attempt_id"] for r in rows})
        add(scope, "DQ rows come from one attempt, the job's", [s.get("attempt_id")], attempts)
        names = sorted(r["check_name"] for r in rows)
        add(scope, "one DQ row per check", sorted([*COUNT_CHECKS, *DRIFT]), names)
        if not rows:
            continue
        first = rows[0]
        tag, config_id, from_tag = first["to_version_tag"], first["config_id"], first["from_version_tag"]
        add(scope, "DQ rows name the job's tag", s.get("tag"), tag)
        target = chunked(tag, config_id)
        bad = recount(target, source[tag], grid[config_id])
        accept = s.get("accept_drift")
        baseline_row = committed.get(first["baseline_run_id"] or "")
        if from_tag:
            add(
                scope,
                "the DQ baseline is a committed run of the from tag",
                from_tag,
                (baseline_row or {}).get("to_version_tag"),
            )
        base_chunks = chunked(from_tag, baseline_row["to_config_id"]) if from_tag and baseline_row else None
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
                observed = want_num / want_den
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

        overridden = [c for c, st in expected_status.items() if st == "overridden"]
        should_commit = "fail" not in expected_status.values() and (accept is None or bool(overridden))
        logged = log_by_run.get(run_id or "", [])
        add(scope, "one run log row", 1, len(logged))
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
                "job outcome = run outcome",
                "succeeded" if should_commit else "failed",
                s.get("status"),
            )
            add(
                scope,
                "dq_override = the reason iff a check was overridden",
                accept if overridden else None,
                row["dq_override"],
            )
            if row["status"] == "failed" and "fail" in expected_status.values():
                add(
                    scope,
                    "a failed gate names the failing checks",
                    True,
                    str(row["error"]).startswith("DqFailed: DQ gate failed"),
                )
            if row["status"] == "committed":
                last_check = max(dt.datetime.fromisoformat(r["checked_at"]) for r in rows)
                finished = dt.datetime.fromisoformat(row["finished_at"])
                add(scope, "checks logged before the commit", True, last_check <= finished)

    # Across the tables.
    pre_gate = set()
    for f in args.pre_gate_runs:
        summary = json.loads(pathlib.Path(f).read_text()).get("summary") or {}
        if summary.get("run_id"):
            pre_gate.add(summary["run_id"])
    add("tables", "DQ rows without a run log row", [], sorted(set(by_run) - set(log_by_run)))
    ungated = []
    for run_id in committed:
        if run_id in pre_gate:
            continue
        rows = by_run.get(run_id, [])
        complete = sorted(r["check_name"] for r in rows) == sorted([*COUNT_CHECKS, *DRIFT])
        if not complete or any(r["status"] == "fail" for r in rows):
            ungated.append(run_id)
    add("tables", "committed runs since the gate without a complete passing DQ set", [], ungated)
    add("tables", "pre-gate runs with DQ rows", [], sorted(pre_gate & set(by_run)))

    out = {
        "label": args.label,
        "git_commit": head,
        "policy": POLICY,
        "dq_rows": len(dq),
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
