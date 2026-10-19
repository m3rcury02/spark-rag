"""Check silver_chunks against a local chunking of git's docs, and save the comparison to runs/.

    uv run --with databricks-sdk==0.152.0 python scripts/m2_verify_silver.py --label m2_x_silver_vs_local \
        --tags v3.5.0 v4.0.0 v4.0.1 --job-runs runs/m2_d[1-9]_*.json

Local side: every tag's docs read straight from git (scripts/gitdocs.py), cleaned and chunked here with every
config of the chunk grid. Silver side: every row of those configs, read on the starter warehouse. Per (tag,
config), the two must agree row for row on path, chunk_id, chunk_index, n_tokens, occurrence, offsets,
section_path, sha256 of body and of embed_text, doc_clean_sha256, commit_sha (git's), doc_content_sha256
(sha256 of git's bytes) and the version and size columns. The docs silver holds plus the expected-zero docs
must be exactly git's docs, and locally every chunk body must be exactly its frame around its offsets'
slice (chunk.rebuilds).

--job-runs: the silver jobs' saved summaries. Each job chunked on serverless and recorded its machine and
the fingerprint of its (path, chunk_id) set; each must equal the fingerprint of the local chunking, so the
chunk IDs do not depend on the machine they were computed on.
"""

import argparse
import hashlib
import json
import pathlib
import platform
from collections import Counter
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from run_sql import run_statement
from runguard import REPO, committed_head

from docs_rag.chunk import CHUNKER_VERSION, chunk_document, rebuilds
from docs_rag.clean import CLEANER_VERSION, clean_page
from docs_rag.config import Config, validate_tag
from docs_rag.silver import fingerprint

FIELDS = (
    "path",
    "chunk_id",
    "chunk_index",
    "n_tokens",
    "occurrence",
    "char_start",
    "char_end",
    "section_path",
    "body_sha256",
    "embed_sha256",
    "doc_clean_sha256",
    "commit_sha",
    "doc_content_sha256",
    "cleaner_version",
    "chunker_version",
    "max_tokens",
    "overlap_tokens",
)
INTS = {"chunk_index", "n_tokens", "occurrence", "char_start", "char_end", "max_tokens", "overlap_tokens"}

ROWS_SQL = """
SELECT version_tag, path, chunk_id, chunk_index, n_tokens, occurrence, char_start, char_end, section_path,
       sha2(body, 256) AS body_sha256, sha2(embed_text, 256) AS embed_sha256, doc_clean_sha256, commit_sha,
       doc_content_sha256, cleaner_version, chunker_version, max_tokens, overlap_tokens
FROM IDENTIFIER(:table) WHERE config_id = :config_id AND source_repo = :repo
"""


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def field(name: str, value: Any) -> Any:
    if name in INTS:
        return int(value)
    if name == "section_path":  # an ARRAY<STRING> comes back as JSON text
        return tuple(json.loads(value))
    return value


def job_fingerprints(paths: list[str]) -> dict[tuple[str, str], dict[str, Any]]:
    """(tag, config_id) -> fingerprint and machine, from the silver jobs' saved runs/ files."""
    out = {}
    for p in paths:
        summary = json.loads(pathlib.Path(p).read_text()).get("summary") or {}
        if summary.get("status") == "succeeded":
            key = (summary["tag"], summary["chunk_config"]["config_id"])
            out[key] = {
                "fingerprint": summary["chunking"]["fingerprint"],
                "machine": summary["environment"]["machine"],
                "file": pathlib.Path(p).name,
            }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True)
    ap.add_argument("--job-runs", nargs="*", default=[], help="runs/ files of the silver jobs")
    ap.add_argument("--catalog", default=Config.catalog)
    ap.add_argument("--schema", default=Config.schema)
    args = ap.parse_args()
    head = committed_head()
    tags = [validate_tag(t) for t in args.tags]
    config = Config(catalog=args.catalog, schema=args.schema)
    jobs = job_fingerprints(args.job_runs)
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
            silver.setdefault(rec["version_tag"], []).append(tuple(field(f, rec[f]) for f in FIELDS))
        for tag in tags:
            local, misframed = [], []
            for path, content in sorted(source[tag].files.items()):
                doc = chunk_document(path, content, chunking)
                text = clean_page(content, fallback_title=path.rsplit("/", 1)[-1].removesuffix(".md")).text
                for c in doc.chunks:
                    if not rebuilds(c, text):
                        misframed.append(f"{path} #{c.chunk_index}")
                    local.append(
                        (path, c.chunk_id, c.chunk_index, c.n_tokens, c.occurrence, c.char_start, c.char_end)
                        + (c.section_path, sha(c.body), sha(c.embed_text), doc.clean_sha256)
                        + (source[tag].commit_sha, sha(content), CLEANER_VERSION, CHUNKER_VERSION)
                        + (chunking.max_tokens, chunking.overlap_tokens)
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
                [list(r) for r in only_local][:3],
            )
            add("rows only in silver", 0, sum(only_silver.values()), [list(r) for r in only_silver][:3])
            add(
                "docs (with chunks, plus expected-zero docs) vs git",
                sorted(source[tag].files),
                sorted(silver_docs),
            )
            add("local chunks whose body does not rebuild from its offsets", 0, len(misframed), misframed[:3])
            job = jobs.get((tag, chunking.config_id))
            if job:
                local_fp = fingerprint((r[0], r[1]) for r in local)
                add(
                    f"fingerprint: job on {job['machine']} ({job['file']}) vs here on {platform.machine()}",
                    local_fp,
                    job["fingerprint"],
                )
    out = {
        "label": args.label,
        "git_commit": head,
        "table": config.silver_table,
        "chunk_configs": [{**c.__dict__, "config_id": c.config_id} for c in config.chunk_grid],
        "local_machine": {"machine": platform.machine(), "python": platform.python_version()},
        "source_commits": {t: source[t].commit_sha for t in tags},
        "silver_rows_read": rows_read,
        "job_runs_compared": sorted(j["file"] for j in jobs.values()),
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
