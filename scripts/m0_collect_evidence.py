"""Collect the M0 smoke-test evidence into runs/, with account identifiers redacted (the repo is public).

Run with:

    uv run --with databricks-sdk==0.152.0 --with databricks-ai-search==0.78 \
        python scripts/m0_collect_evidence.py --run-id <parent run id>

Writes:
- runs/m0_smoke_test.json        the notebook's summary JSON, verbatim (last code cell's output)
- runs/m0_smoke_test_cells.txt   every cell's text output, ANSI codes stripped
- runs/m0_smoke_test_meta.json   run ids, timings, notebook commit, how each file was produced
- runs/m0_index_scan.json        independent check: every row in the index, enumerated with scan()
- runs/m0_sync_pipeline.json     the index's sync pipeline: updates and event log
- runs/m0_workspace_inventory.json  serving endpoints, warehouses, AI Search endpoints, pricing-tier probe
"""

import argparse
import base64
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import urllib.parse
from typing import Any

from databricks.ai_search.client import AISearchClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import ViewsToExport

REPO = pathlib.Path(__file__).resolve().parents[1]
RUNS = REPO / "runs"
ENDPOINT = "docs_rag_endpoint"
INDEX = "workspace.docs_rag_smoke.smoke_idx"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")


class Redactor:
    """Replace account identifiers with placeholders before anything is written to the public repo."""

    def __init__(self, w: WorkspaceClient) -> None:
        host = (w.config.host or "").removeprefix("https://").rstrip("/")
        self.pairs = [
            (w.current_user.me().user_name or "", "<owner>"),
            (host, "<workspace-host>"),
            (str(w.get_workspace_id()), "<workspace-id>"),
        ]

    def text(self, s: str) -> str:
        for old, new in self.pairs:
            if old:
                s = s.replace(old, new)
        s = re.sub(r"s3://[^\s\"'\]]+", "<storage-path>", s)
        return s

    def obj(self, o: Any) -> Any:
        if isinstance(o, str):
            return self.text(o)
        if isinstance(o, list):
            return [self.obj(x) for x in o]
        if isinstance(o, dict):
            return {k: self.obj(v) for k, v in o.items()}
        return o


def iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return (
        dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def write_json(name: str, data: Any) -> None:
    (RUNS / name).write_text(json.dumps(data, indent=2, default=str) + "\n")


def notebook_outputs(w: WorkspaceClient, task_run_id: int) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """Export the run and return [(cell first line, cell text output)] plus the summary JSON."""
    html = w.jobs.export_run(task_run_id, views_to_export=ViewsToExport.CODE).views[0].content or ""
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    if m is None:
        raise RuntimeError("notebook model not found in run export")
    model = json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode()))
    cells: list[tuple[str, str]] = []
    report: dict[str, Any] | None = None
    for cmd in sorted(model.get("commands", []), key=lambda c: c.get("position", 0)):
        first = ((cmd.get("command") or "").strip().splitlines() or [""])[0][:100]
        res = cmd.get("results") or {}
        data = res.get("data")
        chunks = (
            [data]
            if isinstance(data, str)
            else [d.get("data", "") for d in data or [] if isinstance(d, dict)]
        )
        out = ANSI.sub("", "".join(c for c in chunks if isinstance(c, str)))
        cells.append((first, out))
        if '"verdict"' in out and '"finished_utc"' in out:
            report = json.loads(out[out.find("{") :])
    if report is None:
        raise RuntimeError("summary JSON not found in any cell output")
    return cells, report


def scan_index() -> dict[str, Any]:
    client = AISearchClient(
        workspace_url=os.environ["DATABRICKS_HOST"],
        personal_access_token=os.environ["DATABRICKS_TOKEN"],
        disable_notice=True,
    )
    ep = client.get_endpoint(ENDPOINT)
    index = client.get_index(endpoint_name=ENDPOINT, index_name=INDEX)
    desc = index.describe()
    st = desc.get("status", {})
    rows: list[dict[str, Any]] = []
    last = None
    while True:
        page = index.scan(num_results=100, last_primary_key=last)
        data = page.get("data") or []
        for r in data:
            fields: dict[str, Any] = {}
            for f in r.get("fields", []):
                v = f.get("value")
                v = next(iter(v.values())) if isinstance(v, dict) and len(v) == 1 else v
                if isinstance(v, dict) and "values" in v:  # list_value: an embedding vector
                    v = f"<vector len={len(v['values'])}>"
                fields[f.get("key")] = v
            rows.append(fields)
        last = page.get("last_primary_key")
        if not data or not last:
            break
    ids = sorted(str(r.get("id")) for r in rows)
    return {
        "checked_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "method": "AISearchIndex.scan(): pages through every row by primary key, so unlike ANN search it "
        "cannot miss a row that is still in the index",
        "endpoint": {
            "name": ep.get("name"),
            "endpoint_type": ep.get("endpoint_type"),
            "state": ep.get("endpoint_status", {}).get("state"),
            "num_indexes": ep.get("num_indexes"),
        },
        "index": {
            "index_type": desc.get("index_type"),
            "pipeline_type": desc.get("delta_sync_index_spec", {}).get("pipeline_type"),
            "detailed_state": st.get("detailed_state"),
            "ready": st.get("ready"),
            "indexed_row_count": st.get("indexed_row_count"),
        },
        "rows": rows,
        "ids": ids,
        "expected_ids": ["c1", "c2", "c4", "c5"],
        "c3_deleted": "c3" not in ids,
        "c4_updated": any(
            r.get("id") == "c4" and str(r.get("text", "")).startswith("Adaptive Query Execution")
            for r in rows
        ),
    }


def sync_pipeline(w: WorkspaceClient) -> dict[str, Any]:
    pid = w.vector_search_indexes.get_index(INDEX).delta_sync_index_spec.pipeline_id  # type: ignore[union-attr]
    updates = [
        {
            "update_id": u.update_id,
            "cause": u.cause.value if u.cause else None,
            "state": u.state.value if u.state else None,
            "full_refresh": u.full_refresh,
            "created_utc": iso(u.creation_time),
        }
        for u in w.pipelines.list_updates(pid).updates or []
    ]
    raw = w.api_client.do("GET", f"/api/2.0/pipelines/{pid}/events", query={"max_results": 250})
    events = [
        {
            "timestamp": e.get("timestamp"),
            "update_id": (e.get("origin") or {}).get("update_id"),
            "event_type": e.get("event_type"),
            "message": UUID.sub("<principal>", e.get("message") or "")
            if e.get("event_type") == "user_action"
            else e.get("message"),
            "flow_metrics": ((e.get("details") or {}).get("flow_progress") or {}).get("metrics"),
        }
        for e in raw.get("events", [])  # type: ignore[union-attr]
    ]
    events.sort(key=lambda e: e["timestamp"] or "")
    return {
        "pipeline_id": pid,
        "note": "AI Search runs each triggered sync as a pipeline update. flow_metrics.num_output_rows is 0 "
        "for every update, including the initial 5-row sync, so it does not count rows written to the index.",
        "updates": sorted(updates, key=lambda u: u["created_utc"] or ""),
        "events": events,
    }


def inventory(w: WorkspaceClient) -> dict[str, Any]:
    """What the workspace offers, as seen through the API (no resources are created)."""
    out: dict[str, Any] = {
        "checked_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "serving_endpoints": sorted(e.name or "" for e in w.serving_endpoints.list()),
        "sql_warehouses": [
            {"name": x.name, "size": x.cluster_size, "serverless": x.enable_serverless_compute}
            for x in w.warehouses.list()
        ],
        "clusters": len(list(w.clusters.list())),
        "ai_search_endpoints": [
            {"name": e.name, "type": e.endpoint_type.value if e.endpoint_type else None}
            for e in w.vector_search_endpoints.list_endpoints()
        ],
    }
    # Is fair-use consumption observable? Runs one query on the (free) starter warehouse.
    wh = out["sql_warehouses"] and next(iter(w.warehouses.list())).id
    if wh:
        q = "SELECT count(*), min(usage_date), max(usage_date) FROM system.billing.usage"
        r = w.statement_execution.execute_statement(statement=q, warehouse_id=wh, wait_timeout="50s")
        out["billing_usage_probe"] = {
            "query": q,
            "state": r.status.state.value if r.status and r.status.state else None,
            "result": r.result.data_array if r.result else None,
        }
    # IP access lists are a Premium-tier feature, so this probes the pricing tier without changing anything.
    try:
        w.workspace_conf.get_status(keys="enableIpAccessLists")
        out["tier_probe_enableIpAccessLists"] = "available"
    except Exception as e:  # noqa: BLE001 - the error text is the evidence
        out["tier_probe_enableIpAccessLists"] = f"{type(e).__name__}: {e}"[:300]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--run-id", type=int, required=True, help="parent run id printed by m0_submit_smoke_test.py"
    )
    args = ap.parse_args()

    w = WorkspaceClient()
    red = Redactor(w)
    run = w.jobs.get_run(args.run_id)
    task = (run.tasks or [])[0]
    cells, report = notebook_outputs(w, task.run_id)  # type: ignore[arg-type]

    write_json("m0_smoke_test.json", red.obj(report))
    (RUNS / "m0_smoke_test_cells.txt").write_text(
        red.text("\n".join(f"===== cell {i}: {first}\n{out}" for i, (first, out) in enumerate(cells)))
    )
    write_json("m0_index_scan.json", red.obj(scan_index()))
    write_json("m0_sync_pipeline.json", red.obj(sync_pipeline(w)))
    write_json("m0_workspace_inventory.json", red.obj(inventory(w)))

    nb_commit = subprocess.run(
        ["git", "log", "-1", "--format=%H", "--", "notebooks/00_workspace_smoke_test.py"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    meta = {
        "notebook": "notebooks/00_workspace_smoke_test.py",
        "notebook_commit": nb_commit,
        "submitted_by": "scripts/m0_submit_smoke_test.py (Jobs API runs/submit, one notebook task, "
        "no compute or environment spec, so serverless with the workspace default environment)",
        "parent_run_id": args.run_id,
        "task_run_id": task.run_id,
        "result_state": run.state.result_state.value if run.state and run.state.result_state else None,
        "run_start_utc": iso(run.start_time),
        "run_end_utc": iso(run.end_time),
        "run_duration_seconds": round((run.run_duration or 0) / 1000, 1),
        "output_retrieval": "Jobs API runs/export (CODE view); cell outputs parsed from the embedded "
        "notebook model by scripts/m0_collect_evidence.py",
        "redactions": "owner email -> <owner>, workspace host -> <workspace-host>, workspace id -> "
        "<workspace-id>, storage paths -> <storage-path>, principal ids in pipeline events -> <principal>",
    }
    write_json("m0_smoke_test_meta.json", red.obj(meta))
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
