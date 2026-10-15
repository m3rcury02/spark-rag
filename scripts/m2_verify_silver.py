"""Check silver_chunks against a local chunking of git's docs, and save the comparison to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/m2_verify_silver.py --label m2_x_silver_vs_local \
        --tags v3.5.0 v4.0.0 v4.0.1

Local side: every tag's docs read straight from git (scripts/gitdocs.py), cleaned and chunked here with the
default chunk config. Silver side: every row of that config, read on the starter warehouse, with sha2 of its
embed text. Per tag, the two must agree row for row on (path, chunk_id, chunk_index, n_tokens, occurrence,
sha256(embed_text)); the docs silver holds plus the expected-zero docs must be exactly git's docs.

The jobs chunk on serverless (their machine is in each run summary); this script chunks wherever it runs. So
agreement shows the chunk IDs do not depend on the machine, and that text survives the Spark round trip.
"""

import argparse
import hashlib
import json
import pathlib
import platform
import subprocess
import sys
from collections import Counter
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from run_sql import run_statement

from docs_rag.chunk import chunk_document
from docs_rag.config import Config, validate_tag
from docs_rag.silver import fingerprint

REPO = pathlib.Path(__file__).resolve().parents[1]
FIELDS = ("path", "chunk_id", "chunk_index", "n_tokens", "occurrence", "embed_sha256")

ROWS_SQL = """
SELECT version_tag, path, chunk_id, chunk_index, n_tokens, occurrence, sha2(embed_text, 256) AS embed_sha256
FROM IDENTIFIER(:table) WHERE config_id = :config_id AND source_repo = :repo
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True)
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema)
    args = ap.parse_args()
    dirty = ["git", "-C", str(REPO), "status", "--porcelain", "--", ".", ":(exclude)runs"]
    if subprocess.run(dirty, capture_output=True, text=True).stdout:
        sys.exit("uncommitted code changes (runs/ excepted): commit first, so the result can be traced")
    tags = [validate_tag(t) for t in args.tags]
    config = Config(catalog=args.catalog, schema=args.schema)
    chunking = config.chunk

    w = WorkspaceClient()
    warehouse_id = next(iter(w.warehouses.list())).id or ""
    params = {"table": config.silver_table, "config_id": chunking.config_id, "repo": config.source_repo}
    result = run_statement(w, warehouse_id, ROWS_SQL, params)
    silver: dict[str, list[tuple[Any, ...]]] = {}
    for row in result["rows"]:
        rec = dict(zip(result["columns"], row, strict=True))
        silver.setdefault(rec["version_tag"], []).append(
            tuple(int(rec[f]) if f in ("chunk_index", "n_tokens", "occurrence") else rec[f] for f in FIELDS)
        )

    source = read_tags(tags)
    checks: list[dict[str, Any]] = []
    for tag in tags:
        docs = {p: chunk_document(p, c, chunking) for p, c in sorted(source[tag].files.items())}
        local = [
            (
                c.path,
                c.chunk_id,
                c.chunk_index,
                c.n_tokens,
                c.occurrence,
                hashlib.sha256(c.embed_text.encode()).hexdigest(),
            )
            for d in docs.values()
            for c in d.chunks
        ]
        stored = silver.get(tag, [])
        only_local = Counter(local) - Counter(stored)
        only_silver = Counter(stored) - Counter(local)
        silver_docs = {r[0] for r in stored} | (config.zero_chunk_docs & source[tag].files.keys())

        def add(name: str, expected: Any, actual: Any, examples: Any = None, tag: str = tag) -> None:
            c = {"tag": tag, "check": name, "local": expected, "silver": actual, "pass": expected == actual}
            if examples:
                c["examples"] = examples
            checks.append(c)

        add("chunks", len(local), len(stored))
        add(
            "rows only in the local chunking",
            0,
            sum(only_local.values()),
            [list(r) for r in list(only_local)[:3]],
        )
        add("rows only in silver", 0, sum(only_silver.values()), [list(r) for r in list(only_silver)[:3]])
        add(
            "fingerprint of (path, chunk_id)",
            fingerprint((r[0], r[1]) for r in local),
            fingerprint((r[0], r[1]) for r in stored),
        )
        add(
            "docs (with chunks, plus expected-zero docs) vs git",
            sorted(source[tag].files),
            sorted(silver_docs),
        )
    head = ["git", "-C", str(REPO), "rev-parse", "HEAD"]
    out = {
        "label": args.label,
        "git_commit": subprocess.run(head, capture_output=True, text=True).stdout.strip(),
        "table": config.silver_table,
        "chunk_config": {**chunking.__dict__, "config_id": chunking.config_id},
        "local_machine": {"machine": platform.machine(), "python": platform.python_version()},
        "source_commits": {t: source[t].commit_sha for t in tags},
        "silver_rows_read": len(result["rows"]),
        "all_pass": all(c["pass"] for c in checks),
        "checks": checks,
    }
    (REPO / "runs" / f"{args.label}.json").write_text(Redactor(w).dump(out))
    print(json.dumps({"all_pass": out["all_pass"]}, indent=2))
    for c in checks:
        local, stored = (
            (c["local"], c["silver"])
            if not isinstance(c["local"], list)
            else (len(c["local"]), len(c["silver"]))
        )
        print("PASS" if c["pass"] else "FAIL", c["tag"], "|", c["check"], "|", local, "|", stored)


if __name__ == "__main__":
    main()
