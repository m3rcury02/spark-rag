"""Deploy the bundle and run its pipeline job, saving what happened to runs/ (M7, D-016).

    uv run --with databricks-sdk==0.152.0 python scripts/bundle.py deploy --target dev [--label <label>]
    uv run --with databricks-sdk==0.152.0 python scripts/bundle.py run --target dev --label <label> \
        [-- tag=latest initial=false accept_drift="..." accept_drift_of=<run id> with_index=false]

deploy: refuses uncommitted code; builds the wheel (its version names the commit, D-005); creates the target's
schema and artifacts Volume if missing (data stays outside the bundle); runs `databricks bundle deploy` with
the wheel version, which every task asserts it imports; with --label, records the deployment (job id, wheel,
commit, CLI version). GitHub Actions calls it without --label.
run: starts the deployed job with the given job parameters (the rest keep their defaults), waits, and writes
runs/<label>.json: the job run, each task's state, the summary JSON each task wrote to the Volume, and the
error of any failed task. The change-detection task is also written as runs/<label>_detect.json in the shape
of scripts/run_job.py's files (meta + summary), so the M3/M4/M5 verifiers read it as they read those.
Needs DATABRICKS_HOST and DATABRICKS_TOKEN, and the Databricks CLI on PATH (or in $DATABRICKS_CLI).
"""

import argparse
import contextlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from redact import Redactor
from run_job import ARTIFACTS, build_wheel, ensure_volume, git, iso
from runguard import REPO, committed_head

from docs_rag.config import Config

JOB = "docs_pipeline"
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
# The run-id suffix each task passes as --run-id ("<job run id>-<suffix>", resources/pipeline.job.yml).
SUFFIX = {
    "resolve_tag": "resolve",
    "bronze": "bronze",
    "silver_254": "silver254",
    "silver_128": "silver128",
    "change_detect": "detect",
    "sync_index": "sync",
}


def cli() -> str:
    path = os.environ.get("DATABRICKS_CLI") or shutil.which("databricks")
    if not path:
        sys.exit("the Databricks CLI is not on PATH (or set DATABRICKS_CLI)")
    return path


def bundle(*args: str) -> str:
    done = subprocess.run([cli(), "bundle", *args], cwd=REPO, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"databricks bundle {args[0]} failed:\n{done.stderr[-3000:]}")
    return done.stdout


def target_schema(target: str, wheel_version: str) -> str:
    resolved = json.loads(
        bundle("validate", "-t", target, "--var", f"wheel_version={wheel_version}", "-o", "json")
    )
    return str(resolved["variables"]["schema"]["value"])


def deployed_job(target: str) -> dict[str, Any]:
    summary = json.loads(bundle("summary", "-t", target, "-o", "json"))
    job = summary["resources"]["jobs"][JOB]
    if not job.get("id"):
        sys.exit(f"the {target} bundle has no deployed job: deploy first")
    return {"id": int(job["id"]), "name": job.get("name"), "url": job.get("url")}


def deploy(args: argparse.Namespace) -> None:
    head = committed_head()
    wheel = build_wheel()
    version = wheel.name.split("-")[1]
    built = re.search(r"\+g([0-9a-f]{7,40})", version)
    if not built or not head.startswith(built.group(1)) or git("rev-parse", "HEAD") != head:
        sys.exit(f"HEAD moved while building: {head[:9]}, wheel {version}; deploy again")
    w = WorkspaceClient()
    schema = target_schema(args.target, version)
    ensure_volume(w, Config(schema=schema))
    started = time.monotonic()
    out = bundle("deploy", "-t", args.target, "--var", f"wheel_version={version}")
    job = deployed_job(args.target)
    print(out, json.dumps(job), sep="\n")
    if args.label:
        cli_version = subprocess.run([cli(), "--version"], capture_output=True, text=True).stdout.strip()
        record = {
            "label": args.label,
            "git_commit": head,
            "target": args.target,
            "schema": schema,
            "wheel": wheel.name,
            "wheel_version": version,
            "cli": cli_version,
            "job": job,
            "deploy_seconds": round(time.monotonic() - started, 1),
            "deployed_utc": iso(int(time.time() * 1000)),
        }
        (REPO / "runs" / f"{args.label}.json").write_text(Redactor(w).dump(record))


def job_args(raw: list[str]) -> dict[str, str]:
    params = {}
    for item in raw:
        if item == "--":
            continue
        key, sep, value = item.partition("=")
        if not sep:
            sys.exit(f"job parameters are key=value, got {item!r}")
        params[key] = value
    return params


def run(args: argparse.Namespace) -> None:
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        sys.exit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    w = WorkspaceClient()
    job = deployed_job(args.target)
    settings = w.jobs.get(job["id"]).settings
    if settings is None:
        sys.exit(f"job {job['id']} has no settings")
    defaults = {p.name: p.default for p in settings.parameters or []}
    params = job_args(args.job_params)
    unknown = sorted(set(params) - set(defaults))
    if unknown:
        sys.exit(f"unknown job parameters {unknown}; the job takes {sorted(defaults)}")
    expect = next(
        a.split("=", 1)[1]
        for t in settings.tasks or []
        if t.spark_python_task
        for a in (t.spark_python_task.parameters or [])
        if a.startswith("--expect-version=")
    )
    deployed_commit = git("rev-parse", "--verify", f"{expect.split('+g')[-1]}^{{commit}}")
    started = w.jobs.run_now(job_id=job["id"], job_parameters=params)
    print("job run", started.run_id, flush=True)
    while True:  # poll, so a failed run still yields its tasks' output
        r = w.jobs.get_run(started.run_id)
        if r.state and r.state.life_cycle_state and r.state.life_cycle_state.value in TERMINAL:
            break
        time.sleep(20)
    config = Config()
    tasks: dict[str, Any] = {}
    for t in r.tasks or []:
        entry: dict[str, Any] = {
            "task_run_id": t.run_id,
            "state": t.state.result_state.value if t.state and t.state.result_state else None,
            "life_cycle": t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else None,
            "message": t.state.state_message if t.state else None,
            "start_utc": iso(t.start_time),
            "end_utc": iso(t.end_time),
            "seconds": round(t.execution_duration / 1000, 1) if t.execution_duration else None,
        }
        suffix = SUFFIX.get(t.task_key or "")
        if suffix:
            params_ = t.spark_python_task.parameters or [] if t.spark_python_task else []
            schema = next((a.split("=", 1)[1] for a in params_ if a.startswith("--schema=")), Config.schema)
            path = f"{Config(schema=schema).volume(ARTIFACTS)}/summaries/{started.run_id}-{suffix}.json"
            with contextlib.suppress(NotFound):
                entry["summary"] = json.loads(w.files.download(path).contents.read())  # type: ignore[union-attr]
        if entry["state"] not in (None, "SUCCESS", "EXCLUDED") and t.run_id:
            with contextlib.suppress(Exception):
                o = w.jobs.get_run_output(t.run_id)
                entry["error"] = o.error
                entry["error_trace"] = (o.error_trace or "")[-3000:] or None
        if t.condition_task:
            entry["condition"] = {"left": t.condition_task.left, "right": t.condition_task.right}
        tasks[t.task_key or ""] = entry
    meta = {
        "label": args.label,
        "git_commit": deployed_commit,  # the code the job ran: its tasks' --expect-version names it
        "local_head": head,
        "target": args.target,
        "job": job,
        "job_run_id": started.run_id,
        "job_parameters": {**defaults, **params},
        "wheel_version": expect,
        "result_state": r.state.result_state.value if r.state and r.state.result_state else None,
        "state_message": r.state.state_message if r.state else None,
        "run_start_utc": iso(r.start_time),
        "run_end_utc": iso(r.end_time),
        "run_duration_seconds": round(r.run_duration / 1000, 1) if r.run_duration is not None else None,
        "sink": config.sink,
    }
    redactor = Redactor(w)
    out_path.write_text(redactor.dump({"meta": meta, "tasks": tasks}))
    detect = tasks.get("change_detect")
    if detect and detect.get("summary"):
        meta_detect = {**meta, "script": "jobs/change_detect.py", "run_id": detect["summary"].get("run_id")}
        (REPO / "runs" / f"{args.label}_detect.json").write_text(
            redactor.dump({"meta": meta_detect, "summary": detect["summary"], "error": detect.get("error")})
        )
    print(json.dumps({k: v["state"] for k, v in tasks.items()}), meta["result_state"])


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="action", required=True)
    d = sub.add_parser("deploy")
    d.add_argument("--target", required=True, choices=["dev", "prod"])
    d.add_argument("--label", help="also record the deployment as runs/<label>.json")
    r = sub.add_parser("run")
    r.add_argument("--target", required=True, choices=["dev", "prod"])
    r.add_argument("--label", required=True)
    r.add_argument("job_params", nargs=argparse.REMAINDER, help="after --: job parameters as key=value")
    args = ap.parse_args()
    if args.action == "deploy":
        if args.label and (REPO / "runs" / f"{args.label}.json").exists():
            sys.exit(f"runs/{args.label}.json exists: choose a new label")
        deploy(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
