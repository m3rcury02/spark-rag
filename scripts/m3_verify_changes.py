"""Check every change-detection run in ops_run_log against oracles that do not use docs_rag.changes.

    uv run --with databricks-sdk==0.152.0 python scripts/m3_verify_changes.py --label m3_v_changes_vs_git \
        --job-runs runs/m3_*_detect_*.json

For every committed row: the logged doc path lists equal a comparison of git's files at the two tags (read
with scripts/gitdocs.py, compared here with plain dicts); the doc counts equal D-002's git counts where D-002
has the transition; the chunk counts per doc kind equal plain set arithmetic over a local chunking of git's
files (chunk_document, verified in M2); and, where runs/m2_e_stability.json has the transition, the chunk
counts equal its independently coded numbers. For the log as a whole: the committed rows form one chain from
one initial run, failed rows carry no counts and are never a baseline, and every job summary in --job-runs
has exactly its row in the log (and the other way round).
"""

import argparse
import json
import pathlib
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from run_sql import run_statement
from runguard import REPO, committed_head

from docs_rag.chunk import chunk_document
from docs_rag.config import Config

# D-002 (git diff --name-status, reproduced from bronze in runs/m1_h_bronze_vs_git_files.json).
D002 = {("v3.5.0", "v4.0.0"): (19, 96, 51), ("v4.0.0", "v4.0.1"): (1, 4, 0)}
M2E = REPO / "runs" / "m2_e_stability.json"


def chunk_sets(tag_files: dict[str, str], config: Any) -> dict[str, dict[str, int]]:
    """path -> {chunk_id: n_tokens} for one tag."""
    out = {}
    for path, content in tag_files.items():
        out[path] = {c.chunk_id: c.n_tokens for c in chunk_document(path, content, config).chunks}
    return out


def expected(a_files: dict[str, str] | None, b_files: dict[str, str], config: Any) -> dict[str, Any]:
    """The counts a committed row must hold, computed with plain dicts and sets."""
    a_files = a_files or {}
    added = sorted(p for p in b_files if p not in a_files)
    deleted = sorted(p for p in a_files if p not in b_files)
    changed = sorted(p for p in b_files if p in a_files and a_files[p] != b_files[p])
    unchanged = [p for p in b_files if p in a_files and a_files[p] == b_files[p]]
    ca, cb = chunk_sets(a_files, config), chunk_sets(b_files, config)
    ids_a = {i for d in ca.values() for i in d}
    ids_b = {i for d in cb.values() for i in d}
    owner_a = {i: p for p, d in ca.items() for i in d}
    owner_b = {i: p for p, d in cb.items() for i in d}
    new_ids, gone_ids = ids_b - ids_a, ids_a - ids_b
    return {
        "added_paths": added,
        "changed_paths": changed,
        "deleted_paths": deleted,
        "docs_unchanged": len(unchanged),
        "from_chunks": len(ids_a),
        "to_chunks": len(ids_b),
        "chunks_added_new_docs": sum(owner_b[i] in added for i in new_ids),
        "chunks_added_changed_docs": sum(owner_b[i] in changed for i in new_ids),
        "chunks_removed_changed_docs": sum(owner_a[i] in changed for i in gone_ids),
        "chunks_removed_deleted_docs": sum(owner_a[i] in deleted for i in gone_ids),
        "chunks_kept": len(ids_a & ids_b),
        "tokens_to_embed": sum(cb[owner_b[i]][i] for i in new_ids),
        "to_tokens": sum(n for d in cb.values() for n in d.values()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--job-runs", nargs="*", default=[], help="runs/ files of the change_detect jobs")
    args = ap.parse_args()
    head = committed_head()
    config = Config()
    w = WorkspaceClient()
    warehouse = next(iter(w.warehouses.list())).id or ""
    result = run_statement(
        w,
        warehouse,
        "SELECT * FROM IDENTIFIER(:table) WHERE source_repo = :repo AND sink = :sink",
        {"table": config.run_log_table, "repo": config.source_repo, "sink": config.sink},
    )
    rows = [dict(zip(result["columns"], r, strict=True)) for r in result["rows"]]
    for r in rows:
        for name in ("added_paths", "changed_paths", "deleted_paths"):
            if isinstance(r[name], str):
                r[name] = json.loads(r[name])
    checks: list[dict[str, Any]] = []

    def add(scope: str, name: str, want: Any, got: Any) -> None:
        checks.append({"scope": scope, "check": name, "expected": want, "logged": got, "pass": want == got})

    committed = [r for r in rows if r["status"] == "committed"]
    failed = [r for r in rows if r["status"] == "failed"]
    by_id = {r["run_id"]: r for r in committed}

    # The chain: one initial run, each later committed run names the previous one, one head.
    initial = [r for r in committed if r["baseline_run_id"] is None]
    add("log", "committed runs with no baseline (initial)", 1, len(initial))
    order, seen = [], set()
    nxt = {r["baseline_run_id"]: r for r in committed if r["baseline_run_id"]}
    cur = initial[0] if len(initial) == 1 else None
    while cur is not None and cur["run_id"] not in seen:
        order.append(cur)
        seen.add(cur["run_id"])
        cur = nxt.get(cur["run_id"])
    add("log", "committed runs reachable from the initial run, in one chain", len(committed), len(order))
    add(
        "log",
        "failed rows that name a committed baseline or have counts",
        0,
        sum(r["docs_added"] is not None or r["run_id"] in nxt for r in failed),
    )

    # Every committed row against git.
    tags = sorted(
        {r["to_version_tag"] for r in committed}
        | {r["from_version_tag"] for r in committed if r["from_version_tag"]}
    )
    source = read_tags(tags)
    m2e = json.loads(M2E.read_text())["configs"]
    grid = {c.config_id: c for c in config.chunk_grid}
    for r in order:
        a, b = r["from_version_tag"], r["to_version_tag"]
        scope = f"{r['run_id'][:8]} {a or '-'} -> {b}"
        chunking = grid[r["to_config_id"]]
        add(scope, "to commit = git's", source[b].commit_sha, r["to_commit_sha"])
        if a:
            add(scope, "from commit = git's", source[a].commit_sha, r["from_commit_sha"])
            add(
                scope,
                "baseline row is the previous committed run",
                by_id[r["baseline_run_id"]]["to_version_tag"],
                a,
            )
        want = expected(source[a].files if a else None, source[b].files, chunking)
        for name, value in want.items():
            add(scope, name, value, r[name])
        if (a, b) in D002:
            add(
                scope,
                "docs added/changed/deleted = D-002 (git)",
                list(D002[(a, b)]),
                [r["docs_added"], r["docs_changed"], r["docs_deleted"]],
            )
        t = (
            m2e.get(f"{chunking.max_tokens}/{chunking.overlap_tokens}", {})
            .get("transitions", {})
            .get(f"{a}->{b}")
        )
        if t:
            sa = t["section_aware"]
            add(scope, "chunks kept = m2_e kept", sa["kept"], r["chunks_kept"])
            add(
                scope,
                "chunks removed in common docs = m2_e removed",
                sa["removed"],
                r["chunks_removed_changed_docs"] + r["chunks_removed_unchanged_docs"],
            )
            add(
                scope,
                "chunks added = m2_e new_ids_in_b_to_embed",
                sa["new_ids_in_b_to_embed"],
                r["chunks_added_new_docs"]
                + r["chunks_added_changed_docs"]
                + r["chunks_added_unchanged_docs"],
            )
            add(
                scope,
                "tokens to embed = m2_e new_tokens_in_b_to_embed",
                sa["new_tokens_in_b_to_embed"],
                r["tokens_to_embed"],
            )

    # Job summaries and log rows, both ways.
    jobs = {}
    for p in args.job_runs:
        data = json.loads(pathlib.Path(p).read_text())
        summary = data.get("summary") or {}
        if summary.get("run_id"):
            jobs[summary["run_id"]] = (pathlib.Path(p).name, summary.get("status"))
    logged = {r["run_id"]: r["status"] for r in rows}
    for run_id, (name, status) in sorted(jobs.items()):
        outcome = "committed" if status == "succeeded" else "failed"
        add(f"job {name}", "its run log row has the job's outcome", outcome, logged.get(run_id))
    add("log", "log rows without a job summary in --job-runs", [], sorted(set(logged) - set(jobs)))

    out = {
        "label": args.label,
        "git_commit": head,
        "rows": len(rows),
        "committed": len(committed),
        "failed": len(failed),
        "chain": [f"{r['from_version_tag'] or '-'} -> {r['to_version_tag']}" for r in order],
        "checks": checks,
        "all_pass": all(c["pass"] for c in checks),
    }
    text = Redactor(w).dump(out)
    (REPO / "runs" / f"{args.label}.json").write_text(text)
    for c in checks:
        print("PASS" if c["pass"] else "FAIL", c["scope"], "|", c["check"])
    print("all_pass:", out["all_pass"])


if __name__ == "__main__":
    main()
