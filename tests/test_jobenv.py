import argparse
import json
from pathlib import Path
from typing import Any

import pytest

import docs_rag.jobenv as jobenv
from docs_rag.jobenv import base_parser, check_environment, pinned_version, run_with_summary

GOOD = {"databricks-connect": "17.3.14", "docs-rag": "0.0.1.dev5+gabc", "tokenizers": "0.23.3"}


def run(tmp_path: Path, job: Any, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    argv = [
        "job",
        "--run-id",
        "r1",
        "--summary-dir",
        str(tmp_path),
        "--expect-env",
        "4",
        "--expect-version",
        "1",
    ]
    monkeypatch.setattr("sys.argv", argv)
    try:
        run_with_summary(job, base_parser())
    finally:
        summary: dict[str, Any] = json.loads((tmp_path / "r1.json").read_text())
    return summary


def test_summary_is_written_when_the_job_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def job(args: argparse.Namespace, summary: dict[str, Any]) -> None:
        summary["rows"] = 3

    summary = run(tmp_path, job, monkeypatch)
    assert summary["status"] == "succeeded" and summary["rows"] == 3 and summary["finished_utc"]


def test_summary_is_written_and_the_error_raised_when_the_job_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def job(args: argparse.Namespace, summary: dict[str, Any]) -> None:
        summary["step"] = "chunking"
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        run(tmp_path, job, monkeypatch)
    summary = json.loads((tmp_path / "r1.json").read_text())
    assert (summary["status"], summary["error"], summary["step"]) == (
        "failed",
        "ValueError: boom",
        "chunking",
    )


def on_platform(monkeypatch: pytest.MonkeyPatch, runtime: str, **versions: str | None) -> None:
    found = {**GOOD, **{k.replace("_", "-"): v for k, v in versions.items()}}
    monkeypatch.setattr(jobenv, "_version", lambda package: found[package])
    monkeypatch.setattr(jobenv, "pinned_version", lambda package: "0.23.3")
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", runtime)


def test_the_pinned_environment_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    on_platform(monkeypatch, "client.4.10")
    env = check_environment("4", GOOD["docs-rag"])
    assert env["tokenizers"] == "0.23.3" and env["databricks_runtime_version"] == "client.4.10"


@pytest.mark.parametrize(
    "runtime,versions,problem",
    [
        ("client.40.1", {}, "runtime client.40.1"),  # 'client.4' is a prefix of 'client.40'
        ("client.4.10", {"databricks_connect": "16.4.0"}, "databricks-connect 16.4.0"),
        ("client.4.10", {"docs_rag": "0.0.1.dev4+gold"}, "stale cache"),
        ("client.4.10", {"tokenizers": "0.22.1"}, "tokenizers 0.22.1"),
        ("client.4.10", {"databricks_connect": None}, "databricks-connect None"),  # not installed
    ],
)
def test_each_wrong_environment_is_named(
    monkeypatch: pytest.MonkeyPatch, runtime: str, versions: dict[str, str | None], problem: str
) -> None:
    on_platform(monkeypatch, runtime, **versions)
    with pytest.raises(RuntimeError, match="environment check failed") as raised:
        check_environment("4", GOOD["docs-rag"])
    assert problem in str(raised.value)


def test_the_wheel_pins_tokenizers_exactly() -> None:
    assert pinned_version("tokenizers") == "0.23.3"


def test_a_missing_distribution_pins_nothing() -> None:
    assert pinned_version("tokenizers", of="no-such-distribution") is None
    assert pinned_version("no-such-package") is None


def test_switches_and_optional_texts_take_job_parameter_strings() -> None:
    import argparse

    from docs_rag.jobenv import add_flag, flag, optional_text

    ap = argparse.ArgumentParser()
    add_flag(ap, "--initial", "h")
    ap.add_argument("--accept-drift", type=optional_text)
    assert ap.parse_args([]).initial is False
    assert ap.parse_args(["--initial"]).initial is True
    assert ap.parse_args(["--initial=true"]).initial is True
    assert ap.parse_args(["--initial=false"]).initial is False
    assert ap.parse_args(["--initial="]).initial is False  # an empty job parameter
    assert ap.parse_args(["--accept-drift="]).accept_drift is None
    assert ap.parse_args(["--accept-drift=a reason"]).accept_drift == "a reason"
    assert ap.parse_args([]).accept_drift is None
    with pytest.raises(argparse.ArgumentTypeError):
        flag("maybe")
