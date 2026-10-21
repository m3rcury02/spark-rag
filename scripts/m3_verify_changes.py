"""Check every change-detection run in ops_run_log against oracles that do not use docs_rag.changes.

    uv run --with databricks-sdk==0.152.0 python scripts/m3_verify_changes.py --label m3_v_changes_vs_git \
        --job-runs runs/m3_*_detect_*.json

For every committed row: the logged doc path lists equal a comparison of git's files at the two tags (read
with scripts/gitdocs.py, compared here with plain dicts); the doc counts equal D-002's git counts where D-002
has the transition; the chunk counts per doc kind equal plain set arithmetic over a local chunking of git's
files (chunk_document, verified in M2; for a config today's code cannot reproduce, each side's own run's
code, run from a git worktree by scripts/oldcode.py); a run with the re-baseline path logs the baseline
check its code had to make (D-013); and, where
runs/m2_e_stability.json has the transition under the run's config (m2_e chunked with cleaner 1), the chunk
counts equal its independently coded numbers. For the
log as a whole: the committed rows form one chain from one initial run, failed rows carry no counts and are
never a baseline, and every job summary in --job-runs has exactly its row in the log (and the other way
round). The log is read for one chain (--sink): 'none' for M3/M4, the gold table for M5.
"""

import argparse
import json
import pathlib
import subprocess
from typing import Any

import oldcode
from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from run_sql import run_statement
from runguard import REPO, committed_head

from docs_rag.chunk import chunk_document
from docs_rag.config import Config

# The first commit with D-013's re-baseline path (and the baseline_check column).
REBASELINE_COMMIT = "c7b73ac41b93542ecacbe74e48f2a4edaf3dfeb4"
# D-002 (git diff --name-status, reproduced from bronze in runs/m1_h_bronze_vs_git_files.json).
D002 = {("v3.5.0", "v4.0.0"): (19, 96, 51), ("v4.0.0", "v4.0.1"): (1, 4, 0)}
M2E = REPO / "runs" / "m2_e_stability.json"


def descends(commit: str, ancestor: str) -> bool:
    code = subprocess.run(
        ["git", "-C", str(REPO), "merge-base", "--is-ancestor", ancestor, commit], capture_output=True
    ).returncode
    if code not in (0, 1):
        raise RuntimeError(f"git does not know {commit}")
    return code == 0


def chunk_sets(tag_files: dict[str, str], config: Any) -> dict[str, dict[str, int]]:
    """path -> {chunk_id: n_tokens} for one tag."""
    out = {}
    for path, content in tag_files.items():
        out[path] = {c.chunk_id: c.n_tokens for c in chunk_document(path, content, config).chunks}
    return out


def expected(
    a_files: dict[str, str] | None,
    b_files: dict[str, str],
    ca: dict[str, dict[str, int]],
    cb: dict[str, dict[str, int]],
) -> dict[str, Any]:
    """The counts a committed row must hold, computed with plain dicts and sets from both sides' chunks."""
    a_files = a_files or {}
    added = sorted(p for p in b_files if p not in a_files)
    deleted = sorted(p for p in a_files if p not in b_files)
    changed = sorted(p for p in b_files if p in a_files and a_files[p] != b_files[p])
    unchanged = {p for p in b_files if p in a_files and a_files[p] == b_files[p]}
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
        # non-zero only across a config change (D-013's re-baseline path)
        "chunks_added_unchanged_docs": sum(owner_b[i] in unchanged for i in new_ids),
        "chunks_removed_unchanged_docs": sum(owner_a[i] in unchanged for i in gone_ids),
        "chunks_kept": len(ids_a & ids_b),
        "tokens_to_embed": sum(cb[owner_b[i]][i] for i in new_ids),
        "to_tokens": sum(n for d in cb.values() for n in d.values()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--job-runs", nargs="*", default=[], help="runs/ files of the change_detect jobs")
    ap.add_argument("--sink", required=True, help="the chain to check: 'none' (M3/M4) or the gold table (M5)")
    ap.add_argument(
        "--schema", default=Config.schema, help="the target's schema (docs_rag_prod for prod, M7)"
    )
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    config = Config(sink=args.sink, schema=args.schema)
    w = WorkspaceClient()
    warehouse = next(iter(w.warehouses.list())).id or ""
    result = run_statement(
        w,
        warehouse,
        "SELECT * FROM IDENTIFIER(:table) WHERE source_repo = :repo AND sink = :sink",
        {"table": config.run_log_table, "repo": config.source_repo, "sink": config.sink},
    )
    rows = [dict(zip(result["columns"], r, strict=True)) for r in result["rows"]]
    for r in rows:  # the statement API returns every value as a string
        for name, value in r.items():
            if name.endswith("_paths") and isinstance(value, str):
                r[name] = json.loads(value)
            elif (
                name.startswith(
                    (
                        "docs_",
                        "chunks_",
                        "from_docs",
                        "to_docs",
                        "from_chunks",
                        "to_chunks",
                        "tokens_",
                        "to_tokens",
                    )
                )
                and isinstance(value, str)
                and value.isdigit()
            ):
                r[name] = int(value)
    checks: list[dict[str, Any]] = []

    def add(scope: str, name: str, want: Any, got: Any) -> None:
        checks.append({"scope": scope, "check": name, "expected": want, "logged": got, "pass": want == got})

    committed = [r for r in rows if r["status"] == "committed"]
    failed = [r for r in rows if r["status"] == "failed"]
    by_id = {r["run_id"]: r for r in committed}
    add("log", "run_ids with more than one committed row", 0, len(committed) - len(by_id))
    attempts = {i: sum(r["run_id"] == i for r in rows) for i in {r["run_id"] for r in rows}}

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
    m2e_file = json.loads(M2E.read_text())
    m2e = m2e_file["configs"]
    # m2_e's numbers hold for the config its own code computed for each size (cleaner 1), not for a size alone
    m2e_config_id = {
        size: oldcode.chunk_at(m2e_file["git_commit"], {}, *map(int, size.split("/")))[0] for size in m2e
    }
    grid = {c.config_id: c for c in config.chunk_grid}
    cache: dict[tuple[str, str, str], dict[str, dict[str, int]]] = {}

    def sets_of(tag: str, config_id: str, run: dict[str, Any]) -> dict[str, dict[str, int]]:
        """path -> {chunk_id: n_tokens} of a tag under a config: today's code when it can reproduce the
        config, else the code of `run` (the run that logged that side), by its code_version
        (scripts/oldcode.py; runs whose src/ trees are equal share one checkout)."""
        if config_id in grid:
            key = (tag, config_id, "today")
            if key not in cache:
                cache[key] = chunk_sets(source[tag].files, grid[config_id])
            return cache[key]
        commit = oldcode.commit_of(run["code_version"])
        key = (tag, config_id, oldcode.src_tree(commit))
        if key not in cache:
            size = (int(run["max_tokens"]), int(run["overlap_tokens"]))
            got, chunks = oldcode.chunk_at(commit, source[tag].files, *size)
            add(
                f"config {config_id}",
                f"the code of {commit[:10]} (its run's own) computes it",
                config_id,
                got,
            )
            sets: dict[str, dict[str, int]] = {p: {} for p in source[tag].files}
            for c in chunks:
                sets[c["path"]][c["chunk_id"]] = c["n_tokens"]
            cache[key] = sets
        return cache[key]

    for r in order:
        a, b = r["from_version_tag"], r["to_version_tag"]
        scope = f"{r['run_id'][:8]} {a or '-'} -> {b}"
        max_tokens, overlap_tokens = int(r["max_tokens"]), int(r["overlap_tokens"])
        add(scope, "to commit = git's", source[b].commit_sha, r["to_commit_sha"])
        if a:
            add(scope, "from commit = git's", source[a].commit_sha, r["from_commit_sha"])
            add(
                scope,
                "baseline row is the previous committed run",
                by_id[r["baseline_run_id"]]["to_version_tag"],
                a,
            )
        base = by_id.get(r["baseline_run_id"] or "")
        ca = sets_of(a, r["from_config_id"], base) if a and base else {}
        want = expected(source[a].files if a else None, source[b].files, ca, sets_of(b, r["to_config_id"], r))
        if a and base:  # D-013: a run's code says how it checked its baseline (column added with the path)
            commit = oldcode.commit_of(r["code_version"])
            if descends(commit, REBASELINE_COMMIT):
                grid_then = oldcode.grid_config_ids(commit)
                how = "rechunked" if r["from_config_id"] in grid_then else "stored"
                add(scope, "baseline_check is the path its code had to take", how, r.get("baseline_check"))
        for name, value in want.items():
            add(scope, name, value, r[name])
        if (a, b) in D002:
            add(
                scope,
                "docs added/changed/deleted = D-002 (git)",
                list(D002[(a, b)]),
                [r["docs_added"], r["docs_changed"], r["docs_deleted"]],
            )
        same_config = r["from_config_id"] in (None, r["to_config_id"])  # m2_e compares one config's tags
        size = f"{max_tokens}/{overlap_tokens}"
        m2e_config = m2e.get(size, {}) if same_config and r["to_config_id"] == m2e_config_id.get(size) else {}
        t = m2e_config.get("transitions", {}).get(f"{a}->{b}")
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
            # A failed job wrote a 'failed' row only if it says so ("logged"); one that stopped earlier (the
            # environment check, an argument refused before the Spark session, D-011/D-012) logs nothing.
            jobs[summary["run_id"]] = (pathlib.Path(p).name, summary.get("status"), summary.get("logged"))
    logged = {r["run_id"]: r["status"] for r in rows}
    for run_id, (name, status, wrote) in sorted(jobs.items()):
        outcome = "committed" if status == "succeeded" else "failed" if wrote == "failed" else None
        add(f"job {name}", "its run log row has the job's outcome", outcome, logged.get(run_id))
    add("log", "log rows without a job summary in --job-runs", [], sorted(set(logged) - set(jobs)))

    out = {
        "label": args.label,
        "git_commit": head,
        "sink": config.sink,
        "m2e_config_ids": m2e_config_id,
        "rows": len(rows),
        "committed": len(committed),
        "failed": len(failed),
        "chain": [f"{r['from_version_tag'] or '-'} -> {r['to_version_tag']}" for r in order],
        "rows_per_run_id": {i: n for i, n in sorted(attempts.items()) if n > 1},  # attempts of retried runs
        "checks": checks,
        "all_pass": all(c["pass"] for c in checks),
    }
    out_path.write_text(Redactor(w).dump(out))
    for c in checks:
        print("PASS" if c["pass"] else "FAIL", c["scope"], "|", c["check"])
    print("all_pass:", out["all_pass"])


if __name__ == "__main__":
    main()
