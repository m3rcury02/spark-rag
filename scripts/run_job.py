"""Run one pipeline job script as a one-off serverless run and save its summary under runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
        --script jobs/bronze_ingest.py --label m1_v3.5.0_first -- --tag v3.5.0

Steps: build the docs_rag wheel; upload it and the job script to a folder named after the git commit in the
artifacts Volume (serverless can cache a dependency by path, so each commit gets its own path); submit a run
whose job-level environment pins the serverless environment version (D-005) and installs the wheel; wait;
download the summary JSON the job wrote to the Volume; write runs/<label>.json with the run's provenance.

Refuses to run with uncommitted changes, so every file under runs/ points at a commit.
Needs DATABRICKS_HOST and DATABRICKS_TOKEN.
"""

import argparse
import datetime as dt
import io
import json
import pathlib
import subprocess
import sys
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import catalog, compute, jobs

REPO = pathlib.Path(__file__).resolve().parents[1]
CATALOG, SCHEMA, VOLUME = "workspace", "docs_rag_dev", "artifacts"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ENVIRONMENT_VERSION = "4"  # Python 3.12.3 (D-005)
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()


def iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return (
        dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def ensure_volume(w: WorkspaceClient) -> None:
    try:
        w.schemas.get(f"{CATALOG}.{SCHEMA}")
    except NotFound:
        w.schemas.create(name=SCHEMA, catalog_name=CATALOG, comment="docs_rag dev target")
    try:
        w.volumes.read(f"{CATALOG}.{SCHEMA}.{VOLUME}")
    except NotFound:
        w.volumes.create(
            CATALOG, SCHEMA, VOLUME, catalog.VolumeType.MANAGED, comment="wheels, job scripts, run summaries"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True, help="job script, e.g. jobs/bronze_ingest.py")
    ap.add_argument("--label", required=True, help="output name: runs/<label>.json")
    ap.add_argument("job_args", nargs=argparse.REMAINDER, help="arguments after -- go to the job script")
    args = ap.parse_args()
    job_args = [a for a in args.job_args if a != "--"]

    if git("status", "--porcelain"):
        sys.exit("uncommitted changes: commit first, so the run can be traced to a commit")
    commit = git("rev-parse", "HEAD")

    subprocess.run(["uv", "build", "--wheel", "--out-dir", "dist", "-q"], cwd=REPO, check=True)
    wheels = sorted((REPO / "dist").glob("docs_rag-*.whl"), key=lambda p: p.stat().st_mtime)
    wheel = wheels[-1]

    w = WorkspaceClient()
    ensure_volume(w)
    folder = f"{VOLUME_ROOT}/code/{commit[:12]}"
    script = REPO / args.script
    w.files.upload(f"{folder}/{wheel.name}", io.BytesIO(wheel.read_bytes()), overwrite=True)
    w.files.upload(f"{folder}/{script.name}", io.BytesIO(script.read_bytes()), overwrite=True)
    summary_dir = f"{VOLUME_ROOT}/summaries"

    run: Any = w.jobs.submit(
        run_name=f"docs_rag {args.label}",
        timeout_seconds=2 * 3600,
        environments=[
            jobs.JobEnvironment(
                environment_key="default",
                spec=compute.Environment(
                    environment_version=ENVIRONMENT_VERSION, dependencies=[f"{folder}/{wheel.name}"]
                ),
            )
        ],
        tasks=[
            jobs.SubmitTask(
                task_key="main",
                environment_key="default",
                spark_python_task=jobs.SparkPythonTask(
                    python_file=f"{folder}/{script.name}",
                    parameters=[*job_args, "--summary-dir", summary_dir],
                ),
            )
        ],
    )
    while True:  # poll rather than .result(), which raises on failure and would lose the error output
        run = w.jobs.get_run(run.run_id)  # type: ignore[arg-type]
        if run.state and run.state.life_cycle_state and run.state.life_cycle_state.value in TERMINAL:
            break
        time.sleep(20)

    task = (run.tasks or [])[0]
    meta: dict[str, Any] = {
        "label": args.label,
        "git_commit": commit,
        "script": args.script,
        "job_args": job_args,
        "environment_version": ENVIRONMENT_VERSION,
        "wheel": wheel.name,
        "parent_run_id": run.run_id,
        "task_run_id": task.run_id,
        "result_state": run.state.result_state.value if run.state and run.state.result_state else None,
        "run_start_utc": iso(run.start_time),
        "run_end_utc": iso(run.end_time),
        "run_duration_seconds": round(run.run_duration / 1000, 1) if run.run_duration is not None else None,
    }
    output = w.jobs.get_run_output(task.run_id)  # type: ignore[arg-type]
    logs = output.logs or ""
    start = logs.find('{\n  "ingest_run_id"')
    summary: dict[str, Any] | None = json.loads(logs[start : logs.rfind("}") + 1]) if start >= 0 else None
    if summary is not None:  # prefer the copy the job wrote to the Volume (logs can be truncated)
        path = f"{summary_dir}/{summary['ingest_run_id']}.json"
        summary = json.loads(w.files.download(path).contents.read())  # type: ignore[union-attr]
        meta["summary_path"] = path
    out = {
        "meta": meta,
        "summary": summary,
        "error": output.error,
        "error_trace": (output.error_trace or "")[-4000:] or None,
    }
    (REPO / "runs" / f"{args.label}.json").write_text(json.dumps(out, indent=2, default=str) + "\n")
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
