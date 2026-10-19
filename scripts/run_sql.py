"""Run SQL statements on the workspace's (free) starter warehouse and save statements and results to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/run_sql.py --label m1_c1_tamper \
        --sql "UPDATE workspace.docs_rag_dev.bronze_docs SET content_sha256 = 'x' WHERE ..."

Statements run in order and stop at the first failure. Refuses to run with uncommitted changes.
"""

import argparse
import pathlib
import subprocess
import sys
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service.sql import StatementParameterListItem
from redact import Redactor

REPO = pathlib.Path(__file__).resolve().parents[1]


def run_statement(
    w: WorkspaceClient, warehouse_id: str, sql: str, params: dict[str, str] | None = None
) -> dict[str, Any]:
    """Run one statement (with named :parameters) and return all its rows, reading every result chunk."""
    r = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()],
    )
    while r.status and r.status.state and r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id or "")
    state = r.status.state.value if r.status and r.status.state else None
    if state != "SUCCEEDED":
        message = r.status.error.message if r.status and r.status.error else None
        raise SystemExit(f"statement {state}: {message}\n{sql}")
    cols = [c.name for c in (r.manifest.schema.columns or [])] if r.manifest and r.manifest.schema else []
    rows = list((r.result.data_array if r.result else None) or [])
    chunks = (r.manifest.total_chunk_count if r.manifest else None) or 1
    for n in range(1, chunks):
        part = w.statement_execution.get_statement_result_chunk_n(r.statement_id or "", n)
        rows += part.data_array or []
    if r.manifest and r.manifest.total_row_count is not None and len(rows) != r.manifest.total_row_count:
        raise SystemExit(f"read {len(rows)} rows, the result has {r.manifest.total_row_count}")
    return {"sql": sql, "params": params or {}, "columns": cols, "rows": rows}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="output name: runs/<label>.json")
    ap.add_argument("--sql", action="append", required=True, help="statement to run (repeatable)")
    args = ap.parse_args()
    if (REPO / "runs" / f"{args.label}.json").exists():  # runs/ records what happened; never overwrite it
        sys.exit(f"runs/{args.label}.json exists: choose a new label")
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
    results: list[dict[str, Any]] = []
    error = None
    for sql in args.sql:
        try:
            results.append(run_statement(w, warehouse_id, sql))
        except DatabricksError as e:  # e.g. the warehouse refusing work once Free Edition's quota is used up
            error = {"sql": sql, "refused_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            error["error"] = f"{type(e).__name__}: {e}"
            break
    out = {
        "label": args.label,
        "git_commit": subprocess.run(
            [*git, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "statements": results,
        "error": error,
    }
    text = Redactor(w).dump(out)
    (REPO / "runs" / f"{args.label}.json").write_text(text)
    print(text)
    if error:
        sys.exit(f"statement failed, recorded in runs/{args.label}.json")


if __name__ == "__main__":
    main()
