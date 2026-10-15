"""Delete the M0 smoke-test index, table and schema. Keeps the endpoint (Free Edition allows only one).

Run with:

    uv run --with databricks-sdk==0.152.0 python scripts/m0_cleanup.py

Records the before and after state in runs/m0_cleanup.json. The script stops if the schema holds anything
besides the two smoke-test objects, live or dropped. After this,
scripts/m0_collect_evidence.py can no longer be re-run (its index is gone); runs/m0_* is the record.
"""

import datetime as dt
import json
import pathlib
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

RUNS = pathlib.Path(__file__).resolve().parents[1] / "runs"
ENDPOINT = "docs_rag_endpoint"
CATALOG, SCHEMA = "workspace", "docs_rag_smoke"
INDEX, TABLE = f"{CATALOG}.{SCHEMA}.smoke_idx", f"{CATALOG}.{SCHEMA}.smoke_src"
EXPECTED = {"smoke_idx", "smoke_src"}


def now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def sql(w: WorkspaceClient, statement: str) -> list[dict[str, Any]]:
    """Run one statement on the workspace's (free) starter warehouse and return rows as dicts."""
    wh = next(iter(w.warehouses.list())).id
    r = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=wh or "", wait_timeout="50s"
    )
    if not (r.status and r.status.state and r.status.state.value == "SUCCEEDED"):
        raise SystemExit(f"{statement!r} did not succeed: {r.status}")
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    return [dict(zip(cols, row, strict=True)) for row in (r.result.data_array if r.result else None) or []]


def state(w: WorkspaceClient) -> dict[str, Any]:
    schemas = [s.name for s in w.schemas.list(catalog_name=CATALOG)]
    return {
        "checked_utc": now(),
        "endpoints": [
            {
                "name": e.name,
                "num_indexes": e.num_indexes,
                "state": e.endpoint_status.state.value
                if e.endpoint_status and e.endpoint_status.state
                else None,
            }
            for e in w.vector_search_endpoints.list_endpoints()
        ],
        "indexes": [i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=ENDPOINT)],
        "schema_exists": SCHEMA in schemas,
        "schema_objects": sorted(
            t.name or "" for t in w.tables.list(catalog_name=CATALOG, schema_name=SCHEMA)
        )
        if SCHEMA in schemas
        else [],
    }


def main() -> None:
    w = WorkspaceClient()
    before = state(w)
    unexpected = set(before["schema_objects"]) - EXPECTED
    if unexpected:
        raise SystemExit(f"schema holds unexpected objects {sorted(unexpected)}; nothing deleted")

    actions: list[dict[str, Any]] = []
    try:
        w.vector_search_indexes.delete_index(INDEX)
        actions.append({"utc": now(), "action": f"delete_index {INDEX}"})
    except NotFound:
        actions.append({"utc": now(), "action": f"delete_index {INDEX}: already gone"})
    for _ in range(60):  # the index's UC entry disappears asynchronously
        if INDEX not in [i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=ENDPOINT)]:
            break
        time.sleep(10)
    else:
        raise SystemExit("index still listed after 10 min; table and schema left in place")
    actions.append({"utc": now(), "action": "index no longer listed on the endpoint"})

    try:
        w.tables.delete(TABLE)
        actions.append({"utc": now(), "action": f"drop table {TABLE}"})
    except NotFound:
        actions.append({"utc": now(), "action": f"drop table {TABLE}: already gone"})
    if list(w.tables.list(catalog_name=CATALOG, schema_name=SCHEMA)):
        raise SystemExit("schema still lists live tables; schema left in place")
    # Unity Catalog keeps dropped tables (recoverable with UNDROP) and counts them when deciding whether a
    # schema is empty, so a plain DROP SCHEMA fails. Force only when the dropped list holds nothing but the
    # two smoke-test objects. Only name, type and time are recorded: the result also names the owner's email.
    dropped = sql(w, f"SHOW TABLES DROPPED IN {CATALOG}.{SCHEMA}")
    names = {r["tableName"] for r in dropped}
    if not names <= EXPECTED:
        raise SystemExit(
            f"dropped list holds unexpected tables {sorted(names - EXPECTED)}; schema left in place"
        )
    actions.append(
        {
            "utc": now(),
            "action": "SHOW TABLES DROPPED (recoverable with UNDROP until purged)",
            "result": [{k: r[k] for k in ("tableName", "tableType", "deletedAt")} for r in dropped],
        }
    )
    w.schemas.delete(f"{CATALOG}.{SCHEMA}", force=True)
    actions.append(
        {"utc": now(), "action": f"drop schema {CATALOG}.{SCHEMA} (force: purges the dropped tables)"}
    )

    out = {"before": before, "actions": actions, "after": state(w)}
    (RUNS / "m0_cleanup.json").write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
