"""The page both golden tests run on: the fixture page plus sections that force every split path."""

from pathlib import Path

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
    return (HERE / "fixtures" / "spark_page.md").read_text() + EXTRA
