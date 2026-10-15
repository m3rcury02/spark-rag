"""Run SQL statements on the workspace's (free) starter warehouse and save statements and results to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/run_sql.py --label m1_c1_tamper \
        --sql "UPDATE workspace.docs_rag_dev.bronze_docs SET content_sha256 = 'x' WHERE ..."

Statements run in order and stop at the first failure. Refuses to run with uncommitted changes.
"""

import argparse
import pathlib
import subprocess
import sys
from typing import Any

from databricks.sdk import WorkspaceClient
from redact import Redactor

REPO = pathlib.Path(__file__).resolve().parents[1]


def run_statement(w: WorkspaceClient, warehouse_id: str, sql: str) -> dict[str, Any]:
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="50s")
    state = r.status.state.value if r.status and r.status.state else None
    if state != "SUCCEEDED":
        message = r.status.error.message if r.status and r.status.error else None
        raise SystemExit(f"statement {state}: {message}\n{sql}")
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    rows = (r.result.data_array if r.result else None) or []
    return {"sql": sql, "columns": cols, "rows": rows}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="output name: runs/<label>.json")
    ap.add_argument("--sql", action="append", required=True, help="statement to run (repeatable)")
    args = ap.parse_args()
    git = ["git", "-C", str(REPO)]
    if subprocess.run(
        [*git, "status", "--porcelain", "--", ".", ":(exclude)runs"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout:
        sys.exit(
            "uncommitted code changes (runs/ excepted): commit first, so the result can be traced to a commit"
        )

    w = WorkspaceClient()
    warehouse_id = next(iter(w.warehouses.list())).id or ""
    results = [run_statement(w, warehouse_id, sql) for sql in args.sql]
    out = {
        "label": args.label,
        "git_commit": subprocess.run(
            [*git, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "statements": results,
    }
    text = Redactor(w).dump(out)
    (REPO / "runs" / f"{args.label}.json").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
