"""Delete an AI Search endpoint that holds no index, and save the before and after state to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/delete_endpoint.py \
        --endpoint docs_rag_endpoint --label m2_c_delete_idle_endpoint

An idle endpoint is billed while it exists (D-001 Quota), so it is deleted between milestones and created
again when an index needs it. Refuses to delete an endpoint that lists any index. Waits until the endpoint
is no longer listed. Refuses to run on uncommitted code (scripts/runguard.py).
"""

import argparse
import datetime as dt
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from redact import Redactor
from runguard import REPO, committed_head


def now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def endpoints(w: WorkspaceClient) -> list[dict[str, Any]]:
    return [
        {
            "name": e.name,
            "type": e.endpoint_type.value if e.endpoint_type else None,
            "state": e.endpoint_status.state.value if e.endpoint_status and e.endpoint_status.state else None,
            "num_indexes": e.num_indexes,
            "created_utc": dt.datetime.fromtimestamp(e.creation_timestamp / 1000, dt.UTC).isoformat()
            if e.creation_timestamp
            else None,
        }
        for e in w.vector_search_endpoints.list_endpoints()
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--label", required=True, help="output name: runs/<label>.json")
    ap.add_argument("--timeout-s", type=int, default=900)
    args = ap.parse_args()
    head = committed_head()
    w = WorkspaceClient()
    out: dict[str, Any] = {"label": args.label, "git_commit": head, "endpoint": args.endpoint}
    out["before"] = {"utc": now(), "endpoints": endpoints(w)}
    indexes = [i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=args.endpoint)]
    out["before"]["indexes_on_endpoint"] = indexes
    if args.endpoint not in [e["name"] for e in out["before"]["endpoints"]]:
        raise SystemExit(f"{args.endpoint} is not listed; nothing to delete")
    if indexes:
        raise SystemExit(f"{args.endpoint} still holds indexes {indexes}; not deleting")

    out["delete_requested_utc"] = now()
    out["polls"] = []
    try:
        w.vector_search_endpoints.delete_endpoint(endpoint_name=args.endpoint)
        t0 = time.monotonic()
        while True:
            listed = [e for e in endpoints(w) if e["name"] == args.endpoint]
            out["polls"].append({"after_s": round(time.monotonic() - t0, 1), "listed": listed})
            if not listed:
                break
            if time.monotonic() - t0 > args.timeout_s:
                raise SystemExit(f"{args.endpoint} still listed after {args.timeout_s} s: {listed}")
            time.sleep(10)
    finally:  # the record is saved whether or not the deletion finished
        out["after"] = {"utc": now(), "endpoints": endpoints(w)}
        text = Redactor(w).dump(out)
        (REPO / "runs" / f"{args.label}.json").write_text(text)
        print(text)


if __name__ == "__main__":
    main()
