"""Record how many task attempts Databricks made for runs whose summaries are in runs/, and save it to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/task_attempts.py --label m3_k_task_attempts \
        runs/m1_c2_*.json runs/m3_a_*.json ...

Serverless retried failed tasks automatically (D-011), so one run_job.py submission could run its job twice.
This reads each run's task list from the Jobs API (attempt number and result per attempt). Refuses to run on
uncommitted code (scripts/runguard.py).
"""

import argparse
import datetime as dt
import json
import pathlib
from typing import Any

from databricks.sdk import WorkspaceClient
from redact import Redactor
from runguard import REPO, committed_head


def iso(ms: int | None) -> str | None:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).isoformat(timespec="seconds") if ms else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("files", nargs="+", help="runs/ files written by run_job.py")
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    w = WorkspaceClient()
    runs: list[dict[str, Any]] = []
    for f in args.files:
        meta = json.loads(pathlib.Path(f).read_text())["meta"]
        run = w.jobs.get_run(meta["parent_run_id"])
        runs.append(
            {
                "file": pathlib.Path(f).name,
                "parent_run_id": meta["parent_run_id"],
                "attempts": [
                    {
                        "attempt": t.attempt_number,
                        "result": t.state.result_state.value if t.state and t.state.result_state else None,
                        "start_utc": iso(t.start_time),
                        "end_utc": iso(t.end_time),
                    }
                    for t in run.tasks or []
                ],
            }
        )
    out = {"label": args.label, "git_commit": head, "runs": runs}
    text = Redactor(w).dump(out)
    out_path.write_text(text)
    for r in runs:
        print(r["file"], [(a["attempt"], a["result"]) for a in r["attempts"]])


if __name__ == "__main__":
    main()
