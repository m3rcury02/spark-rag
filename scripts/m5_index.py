"""Create, inspect and delete the AI Search endpoint and the gold index (D-014); each call saves a runs/ file.

    uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py create-endpoint --label <label>
    uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py create-index --label <label>
    uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py status --label <label>
    uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py delete-index --label <label>
    # the endpoint, once it holds no index:
    uv run --with databricks-sdk==0.152.0 python scripts/delete_endpoint.py --endpoint docs_rag_endpoint \
        --label <label>

The endpoint is billed while it exists, idle or not (D-001: 4 DBU/h against Free Edition's daily quota), so
it lives only while the index is needed. The index is a TRIGGERED Delta Sync index of the gold table with the
vectors we computed (self-managed embeddings): primary key chunk_id, vector column embedding, and
docs_rag.gold.INDEX_COLUMNS synced beside it. Create it only after gold holds the full backfill: its capacity
is sized from the source table at creation (D-001). create-index waits for the initial sync's own pipeline
update to complete and the index to report nothing pending; jobs/sync_index.py then proves it equals gold.
Refuses to run on uncommitted code.
"""

import argparse
import datetime as dt
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from redact import Redactor
from runguard import REPO, committed_head

from docs_rag.config import Config
from docs_rag.gold import INDEX_COLUMNS
from docs_rag.index_sync import TERMINAL, failed, index_info, summary_of, updates, wait_synced

ENDPOINTS = "/api/2.0/vector-search/endpoints"
INDEXES = "/api/2.0/vector-search/indexes"


def now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def endpoint(w: WorkspaceClient, name: str) -> dict[str, Any] | None:
    try:
        e: dict[str, Any] = w.api_client.do("GET", f"{ENDPOINTS}/{name}")
    except NotFound:
        return None
    return {
        "name": e.get("name"),
        "endpoint_type": e.get("endpoint_type"),
        "state": (e.get("endpoint_status") or {}).get("state"),
        "message": (e.get("endpoint_status") or {}).get("message"),
        "num_indexes": e.get("num_indexes"),
        "creation_timestamp": e.get("creation_timestamp"),
    }


def index(w: WorkspaceClient, name: str) -> dict[str, Any] | None:
    try:
        return summary_of(index_info(w.api_client.do, name))
    except NotFound:
        return None


def poll(what: str, check: Any, timeout_s: float, every_s: float, out: list[dict[str, Any]]) -> Any:
    t0 = time.monotonic()
    while True:
        found, seen = check()
        out.append({"after_s": round(time.monotonic() - t0, 1), "seen": seen})
        if found:
            return round(time.monotonic() - t0, 1)
        if time.monotonic() - t0 > timeout_s:
            raise SystemExit(f"timed out after {timeout_s} s waiting for {what}: {seen}")
        time.sleep(every_s)


def create_endpoint(w: WorkspaceClient, config: Config, out: dict[str, Any]) -> None:
    if endpoint(w, config.endpoint) is not None:
        raise SystemExit(f"{config.endpoint} exists: {endpoint(w, config.endpoint)}")
    out["requested_utc"] = now()
    w.api_client.do("POST", ENDPOINTS, body={"name": config.endpoint, "endpoint_type": "STANDARD"})
    out["polls"] = []

    def online() -> tuple[bool, Any]:
        e = endpoint(w, config.endpoint)
        if e and e["state"] not in ("PROVISIONING", "ONLINE"):
            raise SystemExit(f"{config.endpoint} went {e['state']}: {e}")
        return bool(e and e["state"] == "ONLINE"), e

    out["seconds_to_online"] = poll("the endpoint to be ONLINE", online, 3600, 30, out["polls"])
    out["endpoint"] = endpoint(w, config.endpoint)


def create_index(w: WorkspaceClient, config: Config, out: dict[str, Any]) -> None:
    e = endpoint(w, config.endpoint)
    if not e or e["state"] != "ONLINE":
        raise SystemExit(f"{config.endpoint} is not ONLINE: {e}")
    if index(w, config.index) is not None:
        raise SystemExit(f"{config.index} exists: {index(w, config.index)}")
    spec = {
        "name": config.index,
        "endpoint_name": config.endpoint,
        "primary_key": "chunk_id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": config.gold_table,
            "pipeline_type": "TRIGGERED",
            "columns_to_sync": list(INDEX_COLUMNS),
            "embedding_vector_columns": [
                {"name": "embedding", "embedding_dimension": config.embedding.dimension}
            ],
        },
    }
    out["spec"] = spec
    out["requested_utc"] = now()
    w.api_client.do("POST", INDEXES, body=spec)
    t0 = time.monotonic()
    out["polls"] = []

    def first_update_done() -> tuple[bool, Any]:
        info = index(w, config.index)
        if info:
            failed(info)
        found = updates(w.api_client.do, info["pipeline_id"]) if info and info["pipeline_id"] else []
        if any(u["state"] in ("FAILED", "CANCELED") for u in found):
            raise SystemExit(f"the initial sync failed: {found}")
        done = bool(found) and all(u["state"] in TERMINAL for u in found)
        return done, {"index": info, "updates": found}

    out["seconds_to_first_update_done"] = poll(
        "the initial sync's update", first_update_done, 3600, 30, out["polls"]
    )
    out["index"] = wait_synced(w.api_client.do, config.index)
    out["seconds_to_synced"] = round(time.monotonic() - t0, 1)
    out["updates"] = updates(w.api_client.do, out["index"]["pipeline_id"])


def status(w: WorkspaceClient, config: Config, out: dict[str, Any]) -> None:
    out["endpoint"] = endpoint(w, config.endpoint)
    out["index"] = index(w, config.index)
    if out["index"] and out["index"]["pipeline_id"]:
        out["updates"] = updates(w.api_client.do, out["index"]["pipeline_id"])


def delete_index(w: WorkspaceClient, config: Config, out: dict[str, Any]) -> None:
    out["before"] = index(w, config.index)
    if out["before"] is None:
        raise SystemExit(f"{config.index} does not exist")
    out["requested_utc"] = now()
    w.api_client.do("DELETE", f"{INDEXES}/{config.index}")
    out["polls"] = []

    def gone() -> tuple[bool, Any]:
        info = index(w, config.index)
        return info is None, info

    out["seconds_to_gone"] = poll("the index to be gone", gone, 1800, 15, out["polls"])
    out["endpoint_after"] = endpoint(w, config.endpoint)


ACTIONS = {
    "create-endpoint": create_endpoint,
    "create-index": create_index,
    "status": status,
    "delete-index": delete_index,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=sorted(ACTIONS))
    ap.add_argument("--label", required=True, help="output name: runs/<label>.json")
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    config = Config()
    if config.gold_table is None:
        raise SystemExit("Config.sink is 'none': there is no gold table to index")
    w = WorkspaceClient()
    out: dict[str, Any] = {
        "label": args.label,
        "git_commit": head,
        "action": args.action,
        "endpoint_name": config.endpoint,
        "index_name": config.index,
        "started_utc": now(),
    }
    try:
        ACTIONS[args.action](w, config, out)
        out["status"] = "succeeded"
    except BaseException as e:  # SystemExit too: the record says how far it got
        out["status"] = "failed"
        out["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        out["finished_utc"] = now()
        text = Redactor(w).dump(out)
        out_path.write_text(text)
        print(text)


if __name__ == "__main__":
    main()
