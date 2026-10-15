"""Check bronze_docs against git, an independent source, and save the comparison to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/m1_verify_bronze.py --label m1_f_bronze_vs_git \
        --tags v3.5.0 v4.0.0

Git side (blob-less clone, no full checkout): the commit each tag points at, the number and total bytes of
docs/**/*.md files per tag, and added/modified/deleted .md files between consecutive tags (the D-002 method).
Bronze side (starter warehouse): the same numbers computed from bronze rows alone, plus sha2(content) checks.
Every check is recorded with both values and pass/fail.
"""

import argparse
import json
import pathlib
import subprocess
import sys
import tempfile
from typing import Any

from databricks.sdk import WorkspaceClient
from redact import Redactor

from docs_rag.config import validate_tag

REPO = pathlib.Path(__file__).resolve().parents[1]
SOURCE = "https://github.com/apache/spark"
TABLE = "workspace.docs_rag_dev.bronze_docs"

PER_TAG_SQL = f"""
SELECT version_tag, count(*) AS docs, sum(size_bytes) AS bytes,
       count(DISTINCT commit_sha) AS commits, min(commit_sha) AS commit_sha,
       count_if(sha2(content, 256) <> content_sha256) AS hash_mismatches,
       count_if(octet_length(content) <> size_bytes) AS size_mismatches
FROM {TABLE} GROUP BY version_tag
"""

# Doc-level change between two tags from bronze alone, keyed by path (renames count as delete + add).
DIFF_SQL = f"""
SELECT count_if(a.path IS NULL) AS added,
       count_if(a.path IS NOT NULL AND b.path IS NOT NULL
                AND a.content_sha256 <> b.content_sha256) AS modified,
       count_if(b.path IS NULL) AS deleted
FROM (SELECT path, content_sha256 FROM {TABLE} WHERE version_tag = '{{a}}') a
FULL OUTER JOIN (SELECT path, content_sha256 FROM {TABLE} WHERE version_tag = '{{b}}') b ON a.path = b.path
"""


def git(cwd: str, *args: str) -> str:
    return subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True, check=True).stdout


def git_oracle(tags: list[str]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as d:
        git(d, "init", "-q")
        per_tag: dict[str, Any] = {}
        for tag in tags:
            git(
                d,
                "fetch",
                "-q",
                "--depth",
                "1",
                "--filter=blob:none",
                SOURCE,
                f"refs/tags/{tag}:refs/tags/{tag}",
            )
            listing = git(d, "ls-tree", "-r", "-l", tag, "--", "docs").splitlines()
            md = [line.split(None, 4) for line in listing if line.endswith(".md")]  # mode type sha size path
            per_tag[tag] = {
                "commit_sha": git(d, "rev-parse", f"{tag}^{{commit}}").strip(),
                "docs": len(md),
                "bytes": sum(int(m[3]) for m in md),
            }
        diffs = {}
        for a, b in zip(tags, tags[1:], strict=False):
            status = git(d, "diff", "--no-renames", "--name-status", a, b, "--", "docs/*.md").splitlines()
            letters = [line.split("\t")[0] for line in status]
            diffs[f"{a}->{b}"] = {
                "added": letters.count("A"),
                "modified": letters.count("M"),
                "deleted": letters.count("D"),
            }
    return {"tags": per_tag, "diffs": diffs}


def bronze_side(w: WorkspaceClient, tags: list[str]) -> dict[str, Any]:
    warehouse_id = next(iter(w.warehouses.list())).id or ""

    def query(sql: str) -> list[dict[str, Any]]:
        r = w.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        if not (r.status and r.status.state and r.status.state.value == "SUCCEEDED"):
            raise SystemExit(f"query failed: {r.status}\n{sql}")
        cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
        return [
            dict(zip(cols, row, strict=True)) for row in (r.result.data_array if r.result else None) or []
        ]

    per_tag = {r["version_tag"]: r for r in query(PER_TAG_SQL)}
    diffs = {}
    for a, b in zip(tags, tags[1:], strict=False):
        (row,) = query(DIFF_SQL.format(a=validate_tag(a), b=validate_tag(b)))
        diffs[f"{a}->{b}"] = row
    return {"tags": per_tag, "diffs": diffs}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True, help="tags in load order, e.g. v3.5.0 v4.0.0")
    args = ap.parse_args()
    if subprocess.run(
        ["git", "-C", str(REPO), "status", "--porcelain", "--", ".", ":(exclude)runs"],
        capture_output=True,
        text=True,
    ).stdout:
        sys.exit(
            "uncommitted code changes (runs/ excepted): commit first, so the result can be traced to a commit"
        )

    oracle = git_oracle(args.tags)
    w = WorkspaceClient()
    bronze = bronze_side(w, args.tags)
    checks = []
    for tag in args.tags:
        g, b = oracle["tags"][tag], bronze["tags"].get(tag, {})
        for name, want, got in (
            ("docs", g["docs"], b.get("docs")),
            ("bytes", g["bytes"], b.get("bytes")),
            ("commit_sha", g["commit_sha"], b.get("commit_sha")),
            ("distinct commits", 1, b.get("commits")),
            ("sha2(content) <> content_sha256", 0, b.get("hash_mismatches")),
            ("octet_length(content) <> size_bytes", 0, b.get("size_mismatches")),
        ):
            got = int(got) if isinstance(got, str) and got.isdigit() else got
            checks.append({"tag": tag, "check": name, "git": want, "bronze": got, "pass": want == got})
    for pair, g in oracle["diffs"].items():
        for kind in ("added", "modified", "deleted"):
            got = bronze["diffs"][pair][kind]
            got = int(got) if isinstance(got, str) else got
            checks.append({"tag": pair, "check": kind, "git": g[kind], "bronze": got, "pass": g[kind] == got})
    out = {
        "label": args.label,
        "git_commit": subprocess.run(
            ["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "all_pass": all(c["pass"] for c in checks),
        "checks": checks,
        "git_oracle": oracle,
        "bronze": bronze,
    }
    (REPO / "runs" / f"{args.label}.json").write_text(Redactor(w).dump(out))
    print(json.dumps({"all_pass": out["all_pass"], "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
