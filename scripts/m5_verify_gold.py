"""Check the gold table and its chain against git and the published model, by code written here (D-014).

    uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
        --with torch==2.14.1+cpu --index https://download.pytorch.org/whl/cpu \
        --index-strategy unsafe-best-match python scripts/m5_verify_gold.py --label <label> [--index]

Reads the committed runs of the gold chain (ops_run_log rows of the gold sink, in chain order) and gold's
Delta history. For each run, from a local chunking of git's files at its tag (chunk_document, verified in M2):
- gold at the version its MERGE wrote (time travel) holds exactly the run's chunks, each row's path, section
  path, breadcrumb, body, embed_text and n_tokens equal to git's chunk; a run that changed nothing wrote no
  version, and its chunks equal the previous run's;
- the rows it inserted (embedded_by_run_id) are exactly the chunks added since the previous run, and every
  kept row still carries the run that inserted it before (only added chunks were embedded); the run's logged
  counts, embedded fingerprint (recomputed here) and the history's MERGE metrics agree with that;
- gold's history holds one MERGE per run that changed gold and no other write (Databricks' own OPTIMIZE and
  VACUUM aside).
Vectors, over today's gold, computed in SQL: 384 values, finite, L2 norm within 1e-3 of 1, one model id. A
sample (the 200 rows first by sha2 of chunk_id) is re-embedded here, calling sentence-transformers directly
with the pinned model and revision on this machine's CPU: cosine with gold's vector >= 0.999 each. With
--index: the index's keys (scan) equal gold's, and indexed_row_count gold's row count.
Only configs today's code reproduces are supported (a run under another config is reported, not checked).
"""

import argparse
import hashlib
import importlib.metadata
import json
import math
import platform
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from run_sql import run_statement
from runguard import REPO, committed_head
from sentence_transformers import SentenceTransformer

from docs_rag.chunk import chunk_document
from docs_rag.config import Config
from docs_rag.gold import MAINTENANCE
from docs_rag.index_sync import index_info, scan_keys, summary_of

SAMPLE = 200
MIN_COSINE = 0.999
MAX_NORM_ERROR = 1e-3
TEXT_COLUMNS = ("path", "section_path", "breadcrumb", "body", "embed_text", "n_tokens")


def fingerprint(pairs: set[tuple[str, str]]) -> str:
    """sha256 of the sorted 'path<TAB>chunk_id' lines, as D-011 defines the chunk fingerprint."""
    return hashlib.sha256("".join(sorted(f"{p}\t{c}\n" for p, c in pairs)).encode()).hexdigest()


def as_list(value: Any) -> list[Any]:
    """The statement API returns an array as a JSON string."""
    return list(json.loads(value)) if isinstance(value, str) else list(value)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--index", action="store_true", help="also compare the AI Search index with gold")
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    config = Config()
    gold = config.gold_table
    if gold is None:
        raise SystemExit("Config.sink is 'none': no gold table")
    grid = {c.config_id: c for c in config.chunk_grid}
    w = WorkspaceClient()
    warehouse = next(iter(w.warehouses.list())).id or ""

    def query(sql: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        result = run_statement(w, warehouse, sql, params or {})
        return [dict(zip(result["columns"], r, strict=True)) for r in result["rows"]]

    checks: list[dict[str, Any]] = []

    def add(scope: str, name: str, want: Any, got: Any, ok: bool | None = None) -> None:
        checks.append(
            {
                "scope": scope,
                "check": name,
                "expected": want,
                "logged": got,
                "pass": want == got if ok is None else ok,
            }
        )

    # The chain, in order.
    log = query(
        "SELECT * FROM IDENTIFIER(:table) "
        "WHERE source_repo = :repo AND sink = :sink AND status = 'committed'",
        {"table": config.run_log_table, "repo": config.source_repo, "sink": config.sink},
    )
    by_baseline = {r["baseline_run_id"]: r for r in log}
    chain = []
    run = by_baseline.get(None)
    while run is not None:
        chain.append(run)
        run = by_baseline.get(run["run_id"])
    add("chain", "every committed run is on one chain from one initial run", len(log), len(chain))

    history = sorted(
        query("DESCRIBE HISTORY IDENTIFIER(:table)", {"table": gold}), key=lambda h: int(h["version"])
    )
    writes = [h for h in history if h["operation"] not in ("CREATE TABLE", *MAINTENANCE)]
    add(
        "history",
        "every write to gold is a MERGE of a committed run",
        sorted(int(r["gold_version"]) for r in chain if r["gold_version"] is not None),
        sorted(int(h["version"]) for h in writes if h["operation"] == "MERGE"),
    )
    add("history", "no write but MERGE", [], sorted({h["operation"] for h in writes} - {"MERGE"}))
    metrics_at = {int(h["version"]): json.loads(h["operationMetrics"] or "{}") for h in writes}

    source = read_tags(sorted({r["to_version_tag"] for r in chain}))
    chunks_of: dict[tuple[str, str], dict[str, Any]] = {}

    def expected(tag: str, config_id: str) -> dict[str, Any]:
        """chunk_id -> its text columns, from git at the tag."""
        key = (tag, config_id)
        if key not in chunks_of:
            chunking = grid[config_id]
            chunks_of[key] = {
                c.chunk_id: {
                    "path": c.path,
                    "section_path": list(c.section_path),
                    "breadcrumb": " > ".join(p for p in c.section_path if p),
                    "body": c.body,
                    "embed_text": c.embed_text,
                    "n_tokens": c.n_tokens,
                }
                for path, text in sorted(source[tag].files.items())
                for c in chunk_document(path, text, chunking).chunks
            }
        return chunks_of[key]

    previous: dict[str, Any] = {}  # chunk_id -> git columns of the previous run's chunks
    inserted_by: dict[
        str, str
    ] = {}  # chunk_id -> run that inserted it, as gold held it after the previous run
    for run in chain:
        scope = f"run {run['to_version_tag']} {run['run_id'][:8]}"
        if run["to_config_id"] not in grid:
            add(scope, "its config is one today's code reproduces", True, False)
            break
        want = expected(run["to_version_tag"], run["to_config_id"])
        added = sorted(want.keys() - previous.keys())
        removed = sorted(previous.keys() - want.keys())
        kept = want.keys() & previous.keys()
        pairs = {(want[c]["path"], c) for c in want}
        add(scope, "logged chunk fingerprint = git's", fingerprint(pairs), run["to_chunks_fingerprint"])
        add(
            scope,
            "logged model, chunks embedded, rows inserted and deleted",
            (config.embedding.model_id, len(added), len(added), len(removed)),
            (
                run["embedding_model"],
                int(run["chunks_embedded"]),
                int(run["gold_rows_inserted"]),
                int(run["gold_rows_deleted"]),
            ),
        )
        add(
            scope,
            "logged embedded fingerprint = git's added chunks",
            fingerprint({(want[c]["path"], c) for c in added}),
            run["embedded_ids_fingerprint"],
        )
        if run["gold_version"] is None:
            add(scope, "no gold version: nothing added or removed", (0, 0), (len(added), len(removed)))
            previous = want
            continue
        v = int(run["gold_version"])
        m = metrics_at.get(v, {})
        add(
            scope,
            "history's MERGE metrics: inserted, deleted, updated",
            (len(added), len(removed), 0),
            (
                int(m.get("numTargetRowsInserted", -1)),
                int(m.get("numTargetRowsDeleted", -1)),
                int(m.get("numTargetRowsUpdated", -1)),
            ),
        )
        # v is an int and the table name comes from Config (validated identifiers)
        rows = query(
            f"SELECT chunk_id, {', '.join(TEXT_COLUMNS)}, embedded_by_run_id FROM {gold} VERSION AS OF {v}"
        )
        got = {r["chunk_id"]: r for r in rows}
        add(scope, "gold at its version: one row per chunk_id", len(rows), len(got))
        add(
            scope,
            "gold at its version holds exactly git's chunks",
            sorted(want),
            sorted(got),
            ok=got.keys() == want.keys(),
        )
        differ = [
            c
            for c in want.keys() & got.keys()
            if {
                **{k: got[c][k] for k in TEXT_COLUMNS},
                "section_path": as_list(got[c]["section_path"]),
                "n_tokens": int(got[c]["n_tokens"]),
            }
            != want[c]
        ]
        add(scope, "every row's text columns equal git's chunk", [], sorted(differ)[:5])
        add(
            scope,
            "the rows it inserted are exactly the added chunks",
            added,
            sorted(c for c, r in got.items() if r["embedded_by_run_id"] == run["run_id"]),
        )
        add(
            scope,
            "kept rows were not re-embedded (same inserting run as before)",
            [],
            sorted(c for c in kept & got.keys() if got[c]["embedded_by_run_id"] != inserted_by.get(c))[:5],
        )
        previous = want
        inserted_by = {c: r["embedded_by_run_id"] for c, r in got.items()}

    # Today's gold equals the chain head's state.
    now = query(f"SELECT chunk_id, path FROM {gold}")
    if chain:
        add(
            "gold",
            "today's gold = the chain head's chunks",
            chain[-1]["to_chunks_fingerprint"],
            fingerprint({(r["path"], r["chunk_id"]) for r in now}),
        )

    # Vectors, in SQL.
    stats = query(
        f"""
        SELECT count(*) AS n,
               count_if(size(embedding) <> {config.embedding.dimension}) AS wrong_dimension,
               count_if(exists(embedding, x -> isnan(x) OR abs(x) = double('infinity'))) AS not_finite,
               max(abs(sqrt(aggregate(embedding, 0D, (acc, x) -> acc + double(x) * double(x))) - 1))
                   AS max_norm_error,
               count(DISTINCT embedding_model) AS models,
               min(embedding_model) AS model
        FROM {gold}
        """
    )[0]
    add("vectors", "every vector has the model's dimension", "0", stats["wrong_dimension"])
    add("vectors", "every value is finite", "0", stats["not_finite"])
    add(
        "vectors",
        f"every L2 norm within {MAX_NORM_ERROR} of 1",
        True,
        stats["max_norm_error"],
        ok=stats["max_norm_error"] is not None and float(stats["max_norm_error"]) <= MAX_NORM_ERROR,
    )
    add(
        "vectors",
        "one model, the published one",
        ("1", config.embedding.model_id),
        (stats["models"], stats["model"]),
    )

    # A sample re-embedded here.
    sample = query(
        f"SELECT chunk_id, embed_text, embedding FROM {gold} ORDER BY sha2(chunk_id, 256) LIMIT {SAMPLE}"
    )
    model = SentenceTransformer(config.embedding.model, revision=config.embedding.revision, device="cpu")
    local = model.encode([r["embed_text"] for r in sample], normalize_embeddings=True, batch_size=64)
    cosines = []
    for r, mine in zip(sample, local, strict=True):
        theirs = [float(x) for x in as_list(r["embedding"])]
        dot = sum(float(a) * b for a, b in zip(mine, theirs, strict=True))
        cosines.append(
            dot / (math.sqrt(sum(float(a) ** 2 for a in mine)) * math.sqrt(sum(b * b for b in theirs)))
        )
    add(
        "vectors",
        f"re-embedded sample: cosine >= {MIN_COSINE} for every row",
        len(sample),
        sum(c >= MIN_COSINE for c in cosines),
    )

    index: dict[str, Any] | None = None
    if args.index:
        do = w.api_client.do
        keys = scan_keys(do, config.index, "chunk_id")
        index = summary_of(index_info(do, config.index))
        gold_keys = {r["chunk_id"] for r in now}
        add(
            "index",
            "the index's keys (scan) = gold's",
            len(gold_keys),
            len(keys),
            ok=set(keys) == gold_keys and len(keys) == len(gold_keys),
        )
        add("index", "indexed_row_count = gold's rows", len(gold_keys), index["indexed_row_count"])

    out = {
        "label": args.label,
        "git_commit": head,
        "gold": gold,
        "chain": [
            {
                k: r[k]
                for k in (
                    "run_id",
                    "to_version_tag",
                    "chunks_embedded",
                    "gold_rows_inserted",
                    "gold_rows_deleted",
                    "gold_version",
                )
            }
            for r in chain
        ],
        "history": [{"version": h["version"], "operation": h["operation"]} for h in history],
        "vector_stats": stats,
        "sample": {
            "rows": len(sample),
            "min_cosine": min(cosines, default=None),
            "mean_cosine": sum(cosines) / len(cosines) if cosines else None,
            "machine": platform.machine(),
            "packages": {p: importlib.metadata.version(p) for p, _ in config.embedding.packages},
        },
        "index": index,
        "checks": checks,
        "all_pass": all(c["pass"] for c in checks),
    }
    out_path.write_text(Redactor(w).dump(out))
    for c in checks:
        print("PASS" if c["pass"] else "FAIL", c["scope"], "|", c["check"])
    print("all_pass:", out["all_pass"], f"({sum(c['pass'] for c in checks)}/{len(checks)})")


if __name__ == "__main__":
    main()
