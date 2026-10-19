"""Run one pipeline job script as a one-off serverless run and save its summary under runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
        --script jobs/bronze_ingest.py --label m1_a_v3.5.0_first -- --tag v3.5.0

Steps: build the docs_rag wheel (its version comes from git, so each commit gets its own); upload it and the
job script to a per-commit folder of the artifacts Volume; submit a run whose job-level environment pins the
serverless environment version (D-005) and installs the wheel; wait; read the summary JSON the job wrote to
the Volume; write runs/<label>.json with the run's provenance. The job is told which wheel version and
environment to expect, and fails if it gets anything else.

Refuses to run with uncommitted changes, so every file under runs/ points at a commit.
Needs DATABRICKS_HOST and DATABRICKS_TOKEN.
"""

import argparse
import contextlib
import datetime as dt
import io
import json
import pathlib
import subprocess
import sys
import time
import uuid
from typing import Any

import corpus_fingerprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service import catalog, compute, jobs
from redact import Redactor

from docs_rag.config import Config

REPO = pathlib.Path(__file__).resolve().parents[1]
ARTIFACTS = "artifacts"  # Volume in the target schema: wheels, job scripts, run summaries
# Every job runs on one environment. Versions 3-6 all ship Python 3.12.3; 4 is the most mature (D-005).
ENVIRONMENT_VERSION = "4"
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()


def iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return (
        dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def build_wheel() -> pathlib.Path:
    out = REPO / "dist"
    for old in out.glob("docs_rag-*.whl"):
        old.unlink()
    subprocess.run(["uv", "build", "--wheel", "--out-dir", str(out), "-q"], cwd=REPO, check=True)
    (wheel,) = out.glob("docs_rag-*.whl")
    return wheel


def ensure_volume(w: WorkspaceClient, config: Config) -> None:
    try:
        w.schemas.get(f"{config.catalog}.{config.schema}")
    except NotFound:
        w.schemas.create(name=config.schema, catalog_name=config.catalog, comment="docs_rag target")
    try:
        w.volumes.read(f"{config.catalog}.{config.schema}.{ARTIFACTS}")
    except NotFound:
        w.volumes.create(
            config.catalog,
            config.schema,
            ARTIFACTS,
            catalog.VolumeType.MANAGED,
            comment="wheels, scripts, summaries",
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True, help="job script, e.g. jobs/bronze_ingest.py")
    ap.add_argument("--label", required=True, help="output name: runs/<label>.json")
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema, help="target schema; also forwarded to the job")
    ap.add_argument("job_args", nargs=argparse.REMAINDER, help="arguments after -- go to the job script")
    args = ap.parse_args()
    if (REPO / "runs" / f"{args.label}.json").exists():  # runs/ records what happened; never overwrite it
        sys.exit(f"runs/{args.label}.json exists: choose a new label")
    job_args = [a for a in args.job_args if a != "--"]
    # Catch every spelling argparse would accept for these options: "--schema x", "--schema=x" and prefixes.
    names = {a.split("=", 1)[0] for a in job_args if a.startswith("--")}
    if any(len(n) > 2 and ("--catalog".startswith(n) or "--schema".startswith(n)) for n in names):
        sys.exit("pass --catalog/--schema to run_job.py itself, so artifacts and the job use the same target")
    config = Config(catalog=args.catalog, schema=args.schema)
    volume_root = config.volume(ARTIFACTS)

    if git("status", "--porcelain", "--", ".", ":(exclude)runs"):
        sys.exit(
            "uncommitted code changes (runs/ excepted): commit first, so the run can be traced to a commit"
        )
    commit = git("rev-parse", "HEAD")
    if pathlib.Path(args.script).name == "silver_chunk.py":
        # Silver rows are immutable per (config_id, tag): the code must reproduce the committed corpus golden,
        # or a changed chunking would be stored under the old version's config_id.
        problems = corpus_fingerprint.check()
        if problems:
            sys.exit(
                "corpus golden differs (bump CLEANER_VERSION or CHUNKER_VERSION):\n" + "\n".join(problems)
            )
    wheel = build_wheel()
    wheel_version = wheel.name.split("-")[1]

    w = WorkspaceClient()
    ensure_volume(w, config)
    folder = f"{volume_root}/code/{commit[:12]}"
    script = REPO / args.script
    w.files.upload(f"{folder}/{wheel.name}", io.BytesIO(wheel.read_bytes()), overwrite=True)
    w.files.upload(f"{folder}/{script.name}", io.BytesIO(script.read_bytes()), overwrite=True)
    run_id = str(uuid.uuid4())
    summary_dir = f"{volume_root}/summaries"
    parameters = [
        *job_args,
        *("--catalog", config.catalog, "--schema", config.schema),
        *("--run-id", run_id, "--summary-dir", summary_dir),
        *("--expect-env", ENVIRONMENT_VERSION, "--expect-version", wheel_version),
    ]

    try:
        submitted = w.jobs.submit(
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
                        # A script in a Volume needs the dbfs: URI form; a bare /Volumes/... path is looked up
                        # in the workspace file tree: "Cannot read the python file" (runs/m1_a0_*.json).
                        python_file=f"dbfs:{folder}/{script.name}",
                        parameters=parameters,
                    ),
                )
            ],
        )
    except DatabricksError as e:  # e.g. Free Edition refusing new runs once its quota is used up (D-001)
        record = {"label": args.label, "git_commit": commit, "script": args.script, "job_args": job_args}
        record |= {"submit_refused_utc": iso(int(time.time() * 1000)), "error": f"{type(e).__name__}: {e}"}
        (REPO / "runs" / f"{args.label}.json").write_text(Redactor(w).dump(record))
        sys.exit(f"submit refused, recorded in runs/{args.label}.json: {e}")
    while True:  # poll rather than .result(), which raises on failure and would lose the error output
        run = w.jobs.get_run(submitted.run_id)
        if run.state and run.state.life_cycle_state and run.state.life_cycle_state.value in TERMINAL:
            break
        time.sleep(20)

    task = (run.tasks or [])[0]
    meta: dict[str, Any] = {
        "label": args.label,
        "git_commit": commit,
        "script": args.script,
        "job_args": job_args,
        "target": f"{config.catalog}.{config.schema}",
        "run_id": run_id,
        "environment_version": ENVIRONMENT_VERSION,
        "wheel": wheel.name,
        "parent_run_id": run.run_id,
        "task_run_id": task.run_id,
        "result_state": run.state.result_state.value if run.state and run.state.result_state else None,
        "state_message": run.state.state_message if run.state else None,
        "run_start_utc": iso(run.start_time),
        "run_end_utc": iso(run.end_time),
        "run_duration_seconds": round(run.run_duration / 1000, 1) if run.run_duration is not None else None,
    }
    summary: Any = None  # stays None if the job died before writing it; the error below says why
    with contextlib.suppress(NotFound):
        summary = json.loads(w.files.download(f"{summary_dir}/{run_id}.json").contents.read())  # type: ignore[union-attr]
    output = w.jobs.get_run_output(task.run_id)  # type: ignore[arg-type]
    result = {
        "meta": meta,
        "summary": summary,
        "error": output.error,
        "error_trace": (output.error_trace or "")[-4000:] or None,
    }
    text = Redactor(w).dump(result)
    (REPO / "runs" / f"{args.label}.json").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
