"""The AI Search index as a replica of gold (D-014): REST calls to the index and its sync pipeline, shared by
jobs/sync_index.py (on Databricks) and scripts/m5_index.py (local lifecycle). Every call goes through `do`,
the Databricks SDK's `WorkspaceClient().api_client.do`, so this module needs no SDK version with the newer
AI Search methods and tests can pass a fake.

What D-001 measured shapes it: "ready" is not "synced" (an index reported ready while its first update still
ran), and the sync pipeline's row metrics are always 0, so a sync is proven done by its own pipeline update
reaching COMPLETED, and the index is proven equal to gold by listing every primary key (scan), not by counts.
Gold rows are immutable (a chunk_id fixes its text and, under one model, its vector), so equal keys mean
equal rows.
"""

import time
from collections.abc import Callable
from typing import Any

Do = Callable[..., Any]  # api_client.do(method, path, query=..., body=...)

TERMINAL = ("COMPLETED", "FAILED", "CANCELED")  # a pipeline update's final states
SYNCED = "ONLINE_NO_PENDING_UPDATE"  # a TRIGGERED index with nothing left to apply
_INDEXES = "/api/2.0/vector-search/indexes"


class SyncError(RuntimeError):
    """The index could not be brought to, or shown to be at, gold's state."""


def index_info(do: Do, index: str) -> dict[str, Any]:
    info: dict[str, Any] = do("GET", f"{_INDEXES}/{index}")
    return info


def summary_of(info: dict[str, Any]) -> dict[str, Any]:
    status = info.get("status") or {}
    spec = info.get("delta_sync_index_spec") or {}
    return {
        "detailed_state": status.get("detailed_state"),
        "ready": status.get("ready"),
        "indexed_row_count": status.get("indexed_row_count"),
        "message": status.get("message"),
        "pipeline_id": spec.get("pipeline_id"),
        "source_table": spec.get("source_table"),
        "pipeline_type": spec.get("pipeline_type"),
    }


def updates(do: Do, pipeline_id: str) -> list[dict[str, Any]]:
    """Every update of the index's sync pipeline, oldest first."""
    out: list[dict[str, Any]] = []
    query: dict[str, Any] = {"max_results": 100}
    while True:
        page = do("GET", f"/api/2.0/pipelines/{pipeline_id}/updates", query=query)
        out += [
            {
                "update_id": u.get("update_id"),
                "state": u.get("state"),
                "cause": u.get("cause"),
                "creation_time": u.get("creation_time"),
            }
            for u in page.get("updates") or []
        ]
        token = page.get("next_page_token")
        if not token:
            return sorted(out, key=lambda u: u["creation_time"] or 0)
        query = {"max_results": 100, "page_token": token}


def _poll(
    check: Callable[[], Any],
    what: str,
    timeout: float,
    poll: float,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> Any:
    """Call `check` until it returns something other than None, or fail after `timeout` seconds."""
    deadline = clock() + timeout
    while True:
        found = check()
        if found is not None:
            return found
        if clock() >= deadline:
            raise SyncError(f"timed out after {timeout:.0f} s waiting for {what}")
        sleep(poll)


def wait_idle(
    do: Do,
    pipeline_id: str,
    timeout: float = 3600,
    poll: float = 20,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[dict[str, Any]]:
    """Wait until no update of the pipeline is running (e.g. an index's initial sync); returns the updates."""

    def idle() -> list[dict[str, Any]] | None:
        found = updates(do, pipeline_id)
        return None if any(u["state"] not in TERMINAL for u in found) else found

    result: list[dict[str, Any]] = _poll(idle, "the sync pipeline to be idle", timeout, poll, sleep, clock)
    return result


def sync(
    do: Do,
    index: str,
    pipeline_id: str,
    timeout: float = 3600,
    poll: float = 20,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Trigger one sync and wait for the update it started (the one update that is new after the call) to
    finish; anything but COMPLETED fails. Then wait for the index to report nothing pending."""
    before = {u["update_id"] for u in wait_idle(do, pipeline_id, timeout, poll, sleep, clock)}
    do("POST", f"{_INDEXES}/{index}/sync")

    def started() -> dict[str, Any] | None:
        new = [u for u in updates(do, pipeline_id) if u["update_id"] not in before]
        if len(new) > 1:
            raise SyncError(f"{len(new)} updates started after one sync call: {new}")
        return new[0] if new and new[0]["state"] in TERMINAL else None

    update: dict[str, Any] = _poll(started, "the sync's update to finish", timeout, poll, sleep, clock)
    if update["state"] != "COMPLETED":
        raise SyncError(f"the sync's update ended {update['state']}: {update}")
    wait_synced(do, index, timeout, poll, sleep, clock)
    return update


def wait_synced(
    do: Do,
    index: str,
    timeout: float = 3600,
    poll: float = 20,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    def synced() -> dict[str, Any] | None:
        info = summary_of(index_info(do, index))
        return info if info["detailed_state"] == SYNCED else None

    result: dict[str, Any] = _poll(synced, f"the index to be {SYNCED}", timeout, poll, sleep, clock)
    return result


def _value(v: Any) -> Any:
    """A scan field's value: {'string_value': 'x'} -> 'x'; a null -> None."""
    if isinstance(v, dict) and "null_value" in v:
        return None
    if isinstance(v, dict) and len(v) == 1:
        return next(iter(v.values()))
    return v


def scan_keys(do: Do, index: str, key: str, page: int = 100) -> list[str]:
    """Every primary key in the index, by paging through scan (unlike ANN search it cannot miss a row)."""
    keys: list[str] = []
    last: str | None = None
    while True:
        body: dict[str, Any] = {"num_results": page}
        if last is not None:
            body["last_primary_key"] = last
        resp = do("POST", f"{_INDEXES}/{index}/scan", body=body)
        data = resp.get("data") or []
        for row in data:
            fields = {f.get("key"): _value(f.get("value")) for f in row.get("fields") or []}
            keys.append(str(fields[key]))
        last = resp.get("last_primary_key")
        if not data or not last:
            return keys


def query_keys(do: Do, index: str, key: str, vector: list[float], k: int) -> list[str]:
    """The keys of the index's k nearest rows to `vector` (approximate search)."""
    resp = do(
        "POST",
        f"{_INDEXES}/{index}/query",
        body={"columns": [key], "query_vector": vector, "num_results": k},
    )
    names = [c.get("name") for c in (resp.get("manifest") or {}).get("columns") or []]
    at = names.index(key)
    return [str(row[at]) for row in (resp.get("result") or {}).get("data_array") or []]


def compare_keys(index_keys: list[str], gold_keys: set[str]) -> dict[str, Any]:
    """The index against gold, by primary key: equal, or what is missing, extra or repeated."""
    seen = set(index_keys)
    return {
        "index_rows": len(index_keys),
        "gold_rows": len(gold_keys),
        "missing": sorted(gold_keys - seen)[:5],
        "missing_count": len(gold_keys - seen),
        "extra": sorted(seen - gold_keys)[:5],
        "extra_count": len(seen - gold_keys),
        "repeated_count": len(index_keys) - len(seen),
        "equal": seen == gold_keys and len(index_keys) == len(seen),
    }
