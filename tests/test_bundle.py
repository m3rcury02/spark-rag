"""The bundle (databricks.yml, resources/) against the code it deploys, without a workspace (M7, D-016)."""

from pathlib import Path
from typing import Any

import yaml

from docs_rag.config import Config
from docs_rag.deps import embedding_requirements

ROOT = Path(__file__).resolve().parents[1]
WHEEL = "../dist/docs_rag-*.whl"


def load(path: str) -> Any:
    return yaml.safe_load((ROOT / path).read_text())


JOB = load("resources/pipeline.job.yml")["resources"]["jobs"]["docs_pipeline"]
BUNDLE = load("databricks.yml")
PY_TASKS = [t for t in JOB["tasks"] if "spark_python_task" in t]


def params(task: dict[str, Any]) -> dict[str, str]:
    return dict(p.removeprefix("--").split("=", 1) for p in task["spark_python_task"]["parameters"])


def test_environments_pin_the_wheel_and_the_embedding_packages_of_config() -> None:
    envs = {e["environment_key"]: e["spec"] for e in JOB["environments"]}
    assert {e["environment_version"] for e in envs.values()} == {"4"}  # D-005
    assert envs["default"]["dependencies"] == [WHEEL]
    assert envs["embed"]["dependencies"] == [WHEEL, *embedding_requirements(Config().embedding)]
    assert BUNDLE["artifacts"]["docs_rag"]["files"] == [{"source": "dist/docs_rag-*.whl"}]


def test_every_task_runs_a_job_script_of_the_deployed_wheel_once() -> None:
    for t in PY_TASKS:
        assert (ROOT / "resources" / t["spark_python_task"]["python_file"]).resolve().is_file(), t["task_key"]
        p = params(t)
        assert p["expect-env"] == "4" and p["expect-version"] == "${var.wheel_version}", t["task_key"]
        assert p["schema"] == "${var.schema}" and p["catalog"] == Config().catalog
        assert p["summary-dir"] == "/Volumes/workspace/${var.schema}/artifacts/summaries"
        assert t["max_retries"] == 0  # serverless retried failed tasks once by default (D-011)
    run_ids = [params(t)["run-id"] for t in PY_TASKS]
    assert len(set(run_ids)) == len(run_ids) and all(r.startswith("{{job.run_id}}-") for r in run_ids)


def test_one_tag_for_every_task_and_the_published_config_feeds_change_detection() -> None:
    by_key = {t["task_key"]: t for t in JOB["tasks"]}
    assert params(by_key["resolve_tag"])["tag"] == "{{job.parameters.tag}}"
    for key in ("bronze", "silver_254", "silver_128", "change_detect"):
        assert params(by_key[key])["tag"] == "{{tasks.resolve_tag.values.tag}}"
    published = Config().chunk
    s254 = params(by_key["silver_254"])
    assert (int(s254["max-tokens"]), int(s254["overlap-tokens"])) == (
        published.max_tokens,
        published.overlap_tokens,
    )
    sizes = {(c.max_tokens, c.overlap_tokens) for c in Config().chunk_grid}
    s128 = params(by_key["silver_128"])
    assert (int(s128["max-tokens"]), int(s128["overlap-tokens"])) in sizes
    assert [d["task_key"] for d in by_key["change_detect"]["depends_on"]] == ["silver_254"]
    assert by_key["change_detect"]["environment_key"] == "embed"


def test_switches_come_from_job_parameters_and_the_index_sync_is_opt_in() -> None:
    names = {p["name"]: p["default"] for p in JOB["parameters"]}
    detect = params(next(t for t in PY_TASKS if t["task_key"] == "change_detect"))
    for flag in ("initial", "allow-backwards", "allow-config-change", "accept-drift", "accept-drift-of"):
        name = flag.replace("-", "_")
        assert detect[flag] == "{{job.parameters." + name + "}}"
        assert names[name] in ("false", "")
    assert names["tag"] == "latest" and names["with_index"] == "false"
    cond = next(t for t in JOB["tasks"] if t["task_key"] == "index_requested")["condition_task"]
    assert cond == {"left": "{{job.parameters.with_index}}", "op": "EQUAL_TO", "right": "true"}
    sync = next(t for t in JOB["tasks"] if t["task_key"] == "sync_index")
    assert sync["depends_on"] == [{"task_key": "index_requested", "outcome": "true"}]


def test_one_run_at_a_time_and_nothing_scheduled_until_the_owner_says_so() -> None:
    assert JOB["max_concurrent_runs"] == 1  # two runs could both read the chain head (D-011)
    assert JOB["schedule"]["pause_status"] == "PAUSED"
    for name, target in BUNDLE["targets"].items():
        assert target["presets"]["jobs_max_concurrent_runs"] == 1, name
    assert BUNDLE["targets"]["prod"]["presets"]["trigger_pause_status"] == "PAUSED"
    assert BUNDLE["targets"]["dev"]["variables"]["schema"] == Config().schema
    assert BUNDLE["targets"]["prod"]["variables"]["schema"] == "docs_rag_prod"
