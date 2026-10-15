"""Check silver_chunks against a local chunking of git's docs, and save the comparison to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/m2_verify_silver.py --label m2_x_silver_vs_local \
        --tags v3.5.0 v4.0.0 v4.0.1

Local side: every tag's docs read straight from git (scripts/gitdocs.py), cleaned and chunked here with every
config of the chunk grid. Silver side: every row of those configs, read on the starter warehouse, with sha2 of
its embed text. Per (tag, config), the two must agree row for row on (path, chunk_id, chunk_index, n_tokens,
occurrence, char_start, char_end, doc_clean_sha256, sha256(embed_text)); the docs silver holds plus the
expected-zero docs must be exactly git's docs; and locally every chunk's offsets must point at its slice.

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
from docs_rag.clean import clean_page
from docs_rag.config import Config, validate_tag
from docs_rag.silver import fingerprint

REPO = pathlib.Path(__file__).resolve().parents[1]
FIELDS = (
    "path",
    "chunk_id",
    "chunk_index",
    "n_tokens",
    "occurrence",
    "char_start",
    "char_end",
    "doc_clean_sha256",
    "embed_sha256",
)
INTS = {"chunk_index", "n_tokens", "occurrence", "char_start", "char_end"}

ROWS_SQL = """
SELECT version_tag, path, chunk_id, chunk_index, n_tokens, occurrence, char_start, char_end, doc_clean_sha256,
       sha2(embed_text, 256) AS embed_sha256
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
    w = WorkspaceClient()
    warehouse_id = next(iter(w.warehouses.list())).id or ""
    source = read_tags(tags)
    checks: list[dict[str, Any]] = []
    rows_read = 0
    for chunking in config.chunk_grid:
        params = {"table": config.silver_table, "config_id": chunking.config_id, "repo": config.source_repo}
        result = run_statement(w, warehouse_id, ROWS_SQL, params)
        rows_read += len(result["rows"])
        silver: dict[str, list[tuple[Any, ...]]] = {}
        for row in result["rows"]:
            rec = dict(zip(result["columns"], row, strict=True))
            silver.setdefault(rec["version_tag"], []).append(
                tuple(int(rec[f]) if f in INTS else rec[f] for f in FIELDS)
            )
        for tag in tags:
            local, misplaced = [], []
            for path, content in sorted(source[tag].files.items()):
                doc = chunk_document(path, content, chunking)
                text = clean_page(content, fallback_title=path.rsplit("/", 1)[-1].removesuffix(".md")).text
                for c in doc.chunks:
                    if text[c.char_start : c.char_end] not in c.body:
                        misplaced.append(f"{path} #{c.chunk_index}")
                    sha = hashlib.sha256(c.embed_text.encode()).hexdigest()
                    local.append(
                        (path, c.chunk_id, c.chunk_index, c.n_tokens, c.occurrence, c.char_start, c.char_end)
                        + (doc.clean_sha256, sha)
                    )
            stored = silver.get(tag, [])
            only_local = Counter(local) - Counter(stored)
            only_silver = Counter(stored) - Counter(local)
            silver_docs = {r[0] for r in stored} | (config.zero_chunk_docs & source[tag].files.keys())
            scope = f"{tag} @ {chunking.max_tokens}/{chunking.overlap_tokens}"

            def add(name: str, expected: Any, actual: Any, examples: Any = None, scope: str = scope) -> None:
                c = {
                    "scope": scope,
                    "check": name,
                    "local": expected,
                    "silver": actual,
                    "pass": expected == actual,
                }
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
            add(
                "local chunks whose offsets miss their slice (local invariant)",
                0,
                len(misplaced),
                misplaced[:3],
            )
    head = ["git", "-C", str(REPO), "rev-parse", "HEAD"]
    out = {
        "label": args.label,
        "git_commit": subprocess.run(head, capture_output=True, text=True).stdout.strip(),
        "table": config.silver_table,
        "chunk_configs": [{**c.__dict__, "config_id": c.config_id} for c in config.chunk_grid],
        "local_machine": {"machine": platform.machine(), "python": platform.python_version()},
        "source_commits": {t: source[t].commit_sha for t in tags},
        "silver_rows_read": rows_read,
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
        print("PASS" if c["pass"] else "FAIL", c["scope"], "|", c["check"], "|", local, "|", stored)


if __name__ == "__main__":
    main()
