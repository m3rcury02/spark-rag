"""The page both golden tests run on: the fixture pages plus sections that force every split path, and the
rule for regenerating a golden file."""

import json
import os
from pathlib import Path
from typing import Any

import pytest

HERE = Path(__file__).parent
ROWS = "\n".join(
    f"`spark.example.option{n}` | {n * 10} | "
    + ("Sets how many tasks a shuffle stage runs. " * (1 if n % 5 else 12)).strip()
    + " | 3.0.0"
    for n in range(12)
)
EXTRA = (
    "\n\n## Long paragraph\n\n"
    + " ".join(
        f"Setting spark.sql.shuffle.partitions to {n} changes how many tasks a shuffle stage runs."
        for n in range(30)
    )
    + "\n\n## Long code\n\n```python\n"
    + "\n".join(f"df{n} = spark.range({n}).withColumn('x', col('id') * {n})" for n in range(30))
    + "\n```\n\n## Long table\n\n"
    + "<table><thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since</th></tr></thead>\n"
    + "\n".join(
        f"<tr><td>{c.split(' | ')[0]}</td><td>{c.split(' | ')[1]}</td><td>{c.split(' | ')[2]}</td>"
        f"<td>{c.split(' | ')[3]}</td></tr>"
        for c in ROWS.split("\n")
    )
    + "\n</table>\n\n## Long word\n\n"
    + ("bcdfghjklmnpqrstvwxz" * 5)[:99]  # one word, more tokens than the small config's budget
    + "\n"
)


def page() -> str:
    fixtures = HERE / "fixtures"
    return (
        (fixtures / "spark_page.md").read_text(encoding="utf-8")
        + EXTRA
        + "\n\n"
        + (fixtures / "spark_constructs.md").read_text(encoding="utf-8").split("---\n", 2)[2]
    )


def check_golden(path: Path, current: Any, bump: str) -> None:
    """Compare with the golden file. UPDATE_GOLDEN=1 may only create a missing file: an existing golden is
    the output its version promised, so a change needs a new version (or, for a version that was never
    published to silver, deleting its golden file on purpose)."""
    if os.environ.get("UPDATE_GOLDEN") and not path.exists():
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(current, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    if not path.exists():
        pytest.fail(f"no golden file {path.name}; run with UPDATE_GOLDEN=1 to create it")
    if json.loads(path.read_text(encoding="utf-8")) != current:
        pytest.fail(f"output changed for an existing golden ({path.name}): bump {bump}")
