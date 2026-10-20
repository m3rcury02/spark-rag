from typing import Any

import pytest

from docs_rag.index_sync import SYNCED, SyncError, compare_keys, query_keys, scan_keys, sync


class FakeIndex:
    """The REST surface of one Delta Sync index and its pipeline, advanced one step per updates() call."""

    def __init__(self, keys: list[str], outcome: str = "COMPLETED", extra_updates: int = 0) -> None:
        self.keys = sorted(keys)
        self.updates: list[dict[str, Any]] = [{"update_id": "u0", "state": "COMPLETED", "creation_time": 1}]
        self.outcome = outcome
        self.extra_updates = extra_updates
        self.state = SYNCED
        self.calls: list[tuple[str, str]] = []

    def __call__(self, method: str, path: str, query: Any = None, body: Any = None) -> Any:
        self.calls.append((method, path))
        if path.endswith("/updates"):
            for u in self.updates:  # every poll moves running updates on
                if u["state"] == "RUNNING":
                    u["state"] = self.outcome
                    self.state = SYNCED
                elif u["state"] == "QUEUED":
                    u["state"] = "RUNNING"
            return {"updates": list(self.updates)}
        if path.endswith("/sync"):
            for i in range(1 + self.extra_updates):
                self.updates.append({"update_id": f"s{i}", "state": "QUEUED", "creation_time": 10 + i})
            self.state = "ONLINE_TRIGGERED_UPDATE"
            return {}
        if path.endswith("/scan"):
            start = 0 if "last_primary_key" not in body else self.keys.index(body["last_primary_key"]) + 1
            page = self.keys[start : start + body["num_results"]]
            return {
                "data": [{"fields": [{"key": "chunk_id", "value": {"string_value": k}}]} for k in page],
                "last_primary_key": page[-1] if page else None,
            }
        if path.endswith("/query"):
            return {
                "manifest": {"columns": [{"name": "chunk_id"}, {"name": "score"}]},
                "result": {"data_array": [[k, 0.9] for k in self.keys[: body["num_results"]]]},
            }
        return {"status": {"detailed_state": self.state}, "delta_sync_index_spec": {"pipeline_id": "p"}}


def fast(fake: FakeIndex, timeout: float = 100) -> dict[str, Any]:
    clock = iter(range(10_000))
    return sync(fake, "i", "p", timeout=timeout, poll=1, sleep=lambda _: None, clock=lambda: next(clock))


def test_a_sync_waits_for_its_own_update_to_complete() -> None:
    fake = FakeIndex(["a"])
    assert fast(fake)["update_id"] == "s0"
    assert fake.state == SYNCED
    assert ("POST", "/api/2.0/vector-search/indexes/i/sync") in fake.calls


def test_a_failed_or_ambiguous_sync_is_an_error() -> None:
    with pytest.raises(SyncError, match="ended FAILED"):
        fast(FakeIndex(["a"], outcome="FAILED"))
    with pytest.raises(SyncError, match="2 updates started"):
        fast(FakeIndex(["a"], extra_updates=1))


def test_a_sync_that_never_finishes_times_out() -> None:
    fake = FakeIndex(["a"])
    fake.updates.append({"update_id": "stuck", "state": "WAITING_FOR_RESOURCES", "creation_time": 5})
    with pytest.raises(SyncError, match="timed out"):
        fast(fake, timeout=5)


def test_scan_pages_through_every_key() -> None:
    keys = [f"k{i:03d}" for i in range(250)]
    assert scan_keys(FakeIndex(keys), "i", "chunk_id", page=100) == keys
    assert scan_keys(FakeIndex([]), "i", "chunk_id") == []


def test_query_reads_the_key_column() -> None:
    assert query_keys(FakeIndex(["a", "b", "c"]), "i", "chunk_id", [0.0], 2) == ["a", "b"]


def test_compare_keys() -> None:
    assert compare_keys(["a", "b"], {"a", "b"})["equal"]
    result = compare_keys(["a", "a", "x"], {"a", "b"})
    assert (result["missing"], result["extra"], result["repeated_count"], result["equal"]) == (
        ["b"],
        ["x"],
        1,
        False,
    )
