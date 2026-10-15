"""What every Databricks job script shares: its common arguments, the environment check, and a summary
JSON written whether the job succeeds or fails. Pure Python (no pyspark), importable and testable anywhere.
"""

import argparse
import datetime as dt
import importlib.metadata
import json
import os
import platform
import sys
from collections.abc import Callable
from typing import Any

from docs_rag.config import Config

# Databricks Connect major version per serverless environment version, from the environment release notes
# (https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/, updated 2026-10-12).
DBCONNECT_MAJOR = {"3": "16", "4": "17", "5": "18", "6": "19"}


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def base_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(allow_abbrev=False)  # no prefix matching: options are spelled out in full
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--summary-dir", required=True)
    ap.add_argument("--expect-env", required=True, help="serverless environment version the job must run on")
    ap.add_argument("--expect-version", required=True, help="docs_rag version the job must import")
    return ap


def check_environment(expect_env: str, expect_version: str) -> dict[str, Any]:
    """Fail unless the job runs on the pinned environment with the exact wheel built for this run (D-005)."""
    env: dict[str, Any] = {
        "python": platform.python_version(),
        "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
        "databricks_connect": importlib.metadata.version("databricks-connect"),
        "docs_rag": importlib.metadata.version("docs-rag"),
        "tokenizers": importlib.metadata.version("tokenizers"),
        "machine": platform.machine(),
    }
    want_connect = DBCONNECT_MAJOR.get(expect_env)
    problems = []
    if sys.version_info[:2] != (3, 12):
        problems.append(f"Python {env['python']}, expected 3.12")
    if not str(env["databricks_runtime_version"]).startswith(f"client.{expect_env}."):
        problems.append(f"runtime {env['databricks_runtime_version']}, expected client.{expect_env}.*")
    if env["databricks_connect"].split(".")[0] != want_connect:
        problems.append(f"databricks-connect {env['databricks_connect']}, expected {want_connect}.x")
    if env["docs_rag"] != expect_version:
        problems.append(f"docs_rag {env['docs_rag']} installed, expected {expect_version} (stale cache?)")
    if problems:
        raise RuntimeError("environment check failed: " + "; ".join(problems))
    return env


def run_with_summary(
    job: Callable[[argparse.Namespace, dict[str, Any]], None], parser: argparse.ArgumentParser
) -> None:
    """Parse arguments, run the job, and write <summary-dir>/<run-id>.json even when the job fails."""
    args = parser.parse_args()
    summary: dict[str, Any] = {"run_id": args.run_id, "started_utc": utc_now()}
    try:
        job(args, summary)
        summary["status"] = "succeeded"
    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        summary["finished_utc"] = utc_now()
        os.makedirs(args.summary_dir, exist_ok=True)
        with open(os.path.join(args.summary_dir, f"{args.run_id}.json"), "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(json.dumps(summary, indent=2, default=str))
