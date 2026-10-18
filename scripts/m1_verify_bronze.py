"""Check bronze_docs against git, an independent source, and save the comparison to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/m1_verify_bronze.py --label m1_h_bronze_vs_git \
        --tags v3.5.0 v4.0.0 v4.0.1

Git side (blob-less clone, no checkout): for each tag, the commit it points at and every docs/**/*.md path
with its git blob id and size; added/modified/deleted .md files between consecutive tags (the D-002 method).
Bronze side (starter warehouse): for every row, the git blob id computed from the stored content
(sha1 of "blob <bytes>\\0" + content), so paths and file contents are compared one by one, not just as totals.
If that formula were wrong, every file would mismatch: the check fails loudly rather than passing by accident.

Each check is labelled "vs git" (compared with git) or "invariant" (bronze checked against itself).
"""

import argparse
import json
import subprocess
import tempfile
from typing import Any

from databricks.sdk import WorkspaceClient
from redact import Redactor
from runguard import REPO, committed_head

from docs_rag.config import Config, validate_tag

SOURCE = "https://github.com/apache/spark"

# git's blob id of the stored content: sha1("blob " + <byte length> + NUL + <bytes>).
ROWS_SQL = """
SELECT version_tag, path, commit_sha, size_bytes,
       sha1(concat(encode('blob ', 'UTF-8'), encode(cast(octet_length(content) AS STRING), 'UTF-8'),
                   unhex('00'), encode(content, 'UTF-8'))) AS blob_id,
       sha2(content, 256) = content_sha256 AS hash_ok,
       octet_length(content) = size_bytes AS size_ok
FROM {table} WHERE version_tag IN ({tags})
"""


def git(cwd: str, *args: str) -> str:
    return subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True, check=True).stdout


def git_side(tags: list[str]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as d:
        git(d, "init", "-q")
        per_tag: dict[str, Any] = {}
        for tag in tags:
            ref = f"refs/tags/{tag}:refs/tags/{tag}"
            git(d, "fetch", "-q", "--depth", "1", "--filter=blob:none", SOURCE, ref)
            files = {}
            for line in git(d, "ls-tree", "-r", "-l", tag, "--", "docs").splitlines():
                meta, path = line.split("\t", 1)
                _mode, kind, blob_id, size = meta.split()
                if kind == "blob" and path.endswith(".md"):
                    files[path] = {"blob_id": blob_id, "size": int(size)}
            per_tag[tag] = {"commit_sha": git(d, "rev-parse", f"{tag}^{{commit}}").strip(), "files": files}
        diffs = {}
        for a, b in zip(tags, tags[1:], strict=False):
            status = git(d, "diff", "--no-renames", "--name-status", a, b, "--", "docs/*.md").splitlines()
            letters = [line.split("\t")[0] for line in status]
            diffs[f"{a}->{b}"] = {k: letters.count(k[0].upper()) for k in ("added", "modified", "deleted")}
    return {"tags": per_tag, "diffs": diffs}


def bronze_side(w: WorkspaceClient, table: str, tags: list[str]) -> dict[str, Any]:
    warehouse_id = next(iter(w.warehouses.list())).id or ""
    sql = ROWS_SQL.format(table=table, tags=", ".join(f"'{validate_tag(t)}'" for t in tags))
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="50s")
    if not (r.status and r.status.state and r.status.state.value == "SUCCEEDED"):
        raise SystemExit(f"query failed: {r.status}")
    if r.manifest and r.manifest.total_chunk_count and r.manifest.total_chunk_count > 1:
        raise SystemExit("result spans several chunks; paging is not implemented")
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    per_tag: dict[str, Any] = {t: {"rows": []} for t in tags}
    for row in (r.result.data_array if r.result else None) or []:
        rec = dict(zip(cols, row, strict=True))
        per_tag[rec["version_tag"]]["rows"].append(rec)
    return per_tag


def compare(tags: list[str], g: dict[str, Any], b: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(scope: str, kind: str, name: str, expected: Any, actual: Any, examples: Any = None) -> None:
        c = {"scope": scope, "kind": kind, "check": name, "expected": expected, "bronze": actual}
        if examples:
            c["examples"] = examples
        c["pass"] = expected == actual
        checks.append(c)

    blobs: dict[str, dict[str, str]] = {}
    for tag in tags:
        files, rows = g["tags"][tag]["files"], b[tag]["rows"]
        by_path: dict[str, list[dict[str, Any]]] = {}
        for rec in rows:
            by_path.setdefault(rec["path"], []).append(rec)
        blobs[tag] = {p: recs[0]["blob_id"] for p, recs in by_path.items()}
        missing = sorted(files.keys() - by_path.keys())
        extra = sorted(by_path.keys() - files.keys())
        differ = sorted(p for p in files.keys() & by_path.keys() if blobs[tag][p] != files[p]["blob_id"])
        git_bytes = sum(f["size"] for f in files.values())
        add(tag, "vs git", "docs", len(files), len(rows))
        add(tag, "vs git", "bytes", git_bytes, sum(int(x["size_bytes"]) for x in rows))
        add(
            tag,
            "vs git",
            "commit_sha",
            [g["tags"][tag]["commit_sha"]],
            sorted({x["commit_sha"] for x in rows}),
        )
        add(tag, "vs git", "paths in git but not in bronze", 0, len(missing), missing[:5])
        add(tag, "vs git", "paths in bronze but not in git", 0, len(extra), extra[:5])
        add(tag, "vs git", "paths whose content differs from git's blob", 0, len(differ), differ[:5])
        add(tag, "invariant", "paths stored more than once", 0, sum(len(v) > 1 for v in by_path.values()))
        add(
            tag,
            "invariant",
            "rows where sha2(content) <> content_sha256",
            0,
            sum(x["hash_ok"] != "true" for x in rows),
        )
        add(
            tag,
            "invariant",
            "rows where octet_length <> size_bytes",
            0,
            sum(x["size_ok"] != "true" for x in rows),
        )
    for pair, want in g["diffs"].items():
        a, c = pair.split("->")
        got = {
            "added": len(blobs[c].keys() - blobs[a].keys()),
            "modified": sum(blobs[a][p] != blobs[c][p] for p in blobs[a].keys() & blobs[c].keys()),
            "deleted": len(blobs[a].keys() - blobs[c].keys()),
        }
        for kind in ("added", "modified", "deleted"):
            add(pair, "vs git", f"{kind} (D-002 method)", want[kind], got[kind])
    return checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True, help="tags in load order, e.g. v3.5.0 v4.0.0")
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema)
    args = ap.parse_args()
    commit = committed_head()
    tags = [validate_tag(t) for t in args.tags]
    table = Config(catalog=args.catalog, schema=args.schema).bronze_table

    w = WorkspaceClient()
    g = git_side(tags)
    checks = compare(tags, g, bronze_side(w, table, tags))
    vs_git = [c for c in checks if c["kind"] == "vs git"]
    invariants = [c for c in checks if c["kind"] == "invariant"]
    out = {
        "label": args.label,
        "git_commit": commit,
        "table": table,
        "all_pass": all(c["pass"] for c in checks),
        "summary": {
            "checks_vs_git": f"{sum(c['pass'] for c in vs_git)}/{len(vs_git)}",
            "invariants": f"{sum(c['pass'] for c in invariants)}/{len(invariants)}",
            "files_compared_with_git": sum(len(g["tags"][t]["files"]) for t in tags),
        },
        "method": "bronze blob id = sha1('blob ' + octet_length(content) + NUL + content), as git does",
        "checks": checks,
        # git's per-file listing is kept, so every file-level check can be redone from this file alone
        "git": {t: {"commit_sha": g["tags"][t]["commit_sha"], "files": g["tags"][t]["files"]} for t in tags},
        "git_diffs": g["diffs"],
    }
    (REPO / "runs" / f"{args.label}.json").write_text(Redactor(w).dump(out))
    print(json.dumps({"all_pass": out["all_pass"], **out["summary"]}, indent=2))
    for c in checks:
        status = "PASS" if c["pass"] else "FAIL"
        print(status, c["scope"], "|", c["kind"], "|", c["check"], "|", c["expected"], "|", c["bronze"])


if __name__ == "__main__":
    main()
