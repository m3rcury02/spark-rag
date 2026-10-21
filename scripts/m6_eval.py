"""Run the retrieval eval (M6, D-015): 2 chunk sizes x 2 models, exact search, recall@k and MRR@k.

    uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
        --with torch==2.14.1+cpu --index https://download.pytorch.org/whl/cpu \
        --index-strategy unsafe-best-match python scripts/m6_eval.py --label <label> \
        --set eval/eval_set_v1.json [--write-table] [--check-only]

1. The set: every item's quote occurs in its section of the cleaned text at the set's tag (git, today's
   cleaner, which must be the set's), and resolves to every occurrence in the corpus.
2. Per chunk config of Config.chunk_grid: the tag's docs are chunked here, and the chunks' (path, chunk_id)
   fingerprint must equal silver's for that config and tag (read on the SQL warehouse): the eval ranks the
   pipeline's own chunks. Every item must have a relevant chunk under every config.
   --check-only stops here (used while the set is drafted and reviewed).
3. Per model of Config.eval.models, on this machine's CPU: every chunk's embed text is embedded (normalized,
   as the pipeline does); every question too, with the model's query prefix and, for a model that has one,
   without it. Exact search: cosine over all chunks, so ranks are exact (no ANN).
4. The published config (Config.embedding at Config.chunk) is also ranked with gold's own vectors, read from
   the gold table, and the local vectors are compared with them: the eval of that config is the index's.
5. recall@k and MRR@k with bootstrap CIs, and the paired differences named in D-015, from one set of seeded
   resamples. Writes runs/<label>.json; with --write-table also one eval_results row per item and ranking
   (MERGE on the row's key, so a re-run of the same eval_run_id adds nothing).
Refuses to run on uncommitted code.
"""

import argparse
import datetime as dt
import hashlib
import importlib.metadata
import io
import json
import platform
import time
import uuid
from typing import Any

import numpy as np
from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from run_sql import run_statement
from runguard import REPO, committed_head

from docs_rag.chunk import chunk_document
from docs_rag.clean import CLEANER_VERSION, clean_page
from docs_rag.config import Config
from docs_rag.embedder import encode, load_model
from docs_rag.evaluate import (
    EVAL_RESULTS_KEY,
    ChunkSpan,
    EvalSet,
    bootstrap,
    eval_results_ddl,
    first_relevant_rank,
    hit_at,
    item_violations,
    load_eval_set,
    longest_shared_run,
    occurrences,
    paired_difference,
    reciprocal_rank_at,
    relevant_ids,
    resamples,
)
from docs_rag.sections import split_sections
from docs_rag.silver import fingerprint

ARTIFACTS = "artifacts"


def corpus(
    es: EvalSet,
) -> tuple[Any, dict[str, str], dict[str, dict[tuple[str, ...], list[tuple[int, int]]]]]:
    source = read_tags([es.tag])[es.tag]
    if source.commit_sha != es.commit_sha:
        raise SystemExit(f"{es.tag} is at {source.commit_sha} in git, the set says {es.commit_sha}")
    if es.cleaner_version != CLEANER_VERSION:
        raise SystemExit(
            f"the set's quotes are in cleaner {es.cleaner_version}'s text; today's is {CLEANER_VERSION}"
        )
    texts, spans = {}, {}
    for path, raw in source.files.items():
        page = clean_page(raw, fallback_title=path.rsplit("/", 1)[-1].removesuffix(".md"))
        texts[path] = page.text
        by_path: dict[tuple[str, ...], list[tuple[int, int]]] = {}
        for s in split_sections(page.title, page.text):
            by_path.setdefault(s.path, []).append((s.start, s.start + len(s.body)))
        spans[path] = by_path
    return source, texts, spans


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--set", required=True, help="the eval set, e.g. eval/eval_set_v1.json")
    ap.add_argument("--write-table", action="store_true", help="also MERGE the rows into eval_results")
    ap.add_argument(
        "--check-only", action="store_true", help="validate the set and the corpus, embed nothing"
    )
    ap.add_argument(
        "--table", default="eval_results", help="the results table (another name for a smoke test)"
    )
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    w = WorkspaceClient()
    redactor = Redactor(w)
    warehouse = next(iter(w.warehouses.list())).id or ""
    config = Config()
    ev = config.eval
    set_text = (REPO / args.set).read_text()
    set_sha = hashlib.sha256(set_text.encode()).hexdigest()
    es = load_eval_set(set_text)
    out: dict[str, Any] = {
        "label": args.label,
        "git_commit": head,
        "eval_set": {"file": args.set, "version": es.version, "sha256": set_sha, "items": len(es.items)},
        "eval_config": {k: v for k, v in vars(ev).items() if k != "models"},
        "started_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    }

    # 1. The set against the cleaned text.
    source, texts, spans = corpus(es)
    found = {}
    problems = {}
    for it in es.items:
        p = item_violations(it, texts, spans.get(it.path, {}), ev.quote_chars)
        if p:
            problems[it.item_id] = p
        found[it.item_id] = occurrences(it.quote, texts)
    out["items"] = {
        it.item_id: {
            "path": it.path,
            "kind": it.kind,
            "quote_chars": len(it.quote),
            "occurrences": len(found[it.item_id]),
            "docs_with_the_quote": len({o.path for o in found[it.item_id]}),
            "also_in": list(it.also_in),
            "longest_shared_word_run": longest_shared_run(it.question, it.quote),
        }
        for it in es.items
    }
    if problems:
        out["problems"] = problems
        out_path.write_text(redactor.dump(out))
        raise SystemExit(f"items fail their checks: {problems}")

    # 2. Each config's chunks, pinned to silver's; every item has a relevant chunk.
    configs: dict[str, dict[str, Any]] = {}
    for chunking in config.chunk_grid:
        chunks = [
            c
            for path, raw in sorted(source.files.items())
            for c in chunk_document(path, raw, chunking).chunks
        ]
        rows = run_statement(
            w,
            warehouse,
            "SELECT path, chunk_id FROM IDENTIFIER(:table) "
            "WHERE config_id = :config AND source_repo = :repo AND version_tag = :tag",
            {
                "table": config.silver_table,
                "config": chunking.config_id,
                "repo": config.source_repo,
                "tag": es.tag,
            },
        )["rows"]
        local_fp = fingerprint((c.path, c.chunk_id) for c in chunks)
        silver_fp = fingerprint((r[0], r[1]) for r in rows)
        spans_ = [ChunkSpan(c.chunk_id, c.path, c.char_start, c.char_end) for c in chunks]
        relevant = {
            it.item_id: relevant_ids(found[it.item_id], spans_, ev.min_quote_share) for it in es.items
        }
        configs[chunking.config_id] = {"chunking": chunking, "chunks": chunks, "relevant": relevant}
        out.setdefault("configs", {})[chunking.config_id] = {
            "max_tokens": chunking.max_tokens,
            "overlap_tokens": chunking.overlap_tokens,
            "chunks": len(chunks),
            "silver_rows": len(rows),
            "fingerprint_equals_silver": local_fp == silver_fp,
            "relevant_chunks_per_item": {k: len(v) for k, v in relevant.items()},
        }
        if local_fp != silver_fp:
            out_path.write_text(redactor.dump(out))
            raise SystemExit(f"the local chunking of {es.tag} under {chunking.config_id} is not silver's")
        unlabelled = sorted(k for k, v in relevant.items() if not v)
        if unlabelled:
            out_path.write_text(redactor.dump(out))
            raise SystemExit(f"items with no relevant chunk under {chunking.config_id}: {unlabelled}")
    if args.check_only:
        out["check_only"] = True
        out_path.write_text(redactor.dump(out))
        print("checks pass:", len(es.items), "items;", {k: v["chunks"] for k, v in out["configs"].items()})
        return

    # 3./4. Embed and rank.
    questions = [it.question for it in es.items]
    rankings: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    timings: dict[str, Any] = {}

    def rank_all(key: tuple[str, str, str, str], q: np.ndarray, c: np.ndarray, info: dict[str, Any]) -> None:
        ids = [x.chunk_id for x in info["chunks"]]
        scores = q @ c.T
        rows = []
        for i, it in enumerate(es.items):
            order = np.argsort(-scores[i], kind="stable")
            ranked = [ids[j] for j in order]
            rank = first_relevant_rank(ranked, info["relevant"][it.item_id])
            rows.append(
                {
                    "item_id": it.item_id,
                    "rank": rank,
                    "hit": hit_at(rank, ev.recall_k),
                    "rr": reciprocal_rank_at(rank, ev.mrr_k),
                    "top": ranked[: ev.mrr_k],
                    "relevant": sorted(info["relevant"][it.item_id]),
                }
            )
        rankings[key] = rows

    published = (config.embedding.model_id, config.chunk.config_id)
    for model_cfg in ev.models:
        t0 = time.monotonic()
        model = load_model(model_cfg)
        prefixes = [model_cfg.query_prefix] + ([""] if model_cfg.query_prefix else [])
        qv = {p: np.asarray(encode(model, [p + q for q in questions], model_cfg)) for p in prefixes}
        timings[model_cfg.model_id] = {"load_and_queries_seconds": round(time.monotonic() - t0, 1)}
        for config_id, info in configs.items():
            t1 = time.monotonic()
            cv = np.asarray(encode(model, [c.embed_text for c in info["chunks"]], model_cfg))
            seconds = time.monotonic() - t1
            timings[model_cfg.model_id][config_id] = {
                "chunks": len(info["chunks"]),
                "encode_seconds": round(seconds, 1),
                "chunks_per_second": round(len(info["chunks"]) / seconds, 1),
            }
            print(model_cfg.model, config_id, timings[model_cfg.model_id][config_id], flush=True)
            for p in prefixes:
                rank_all((model_cfg.model_id, p, config_id, "local"), qv[p], cv, info)
            if (model_cfg.model_id, config_id) == published:
                out["gold_vectors"] = gold_check(w, warehouse, config, info, cv)
                gv = np.asarray(out["gold_vectors"].pop("_vectors"))
                rank_all((model_cfg.model_id, prefixes[0], config_id, "gold"), qv[prefixes[0]], gv, info)
    out["timings"] = timings

    # 5. Metrics, CIs and paired differences.
    draws = resamples(len(es.items), ev.resamples, ev.seed)
    name = {m.model_id: m.model.split("/")[-1] for m in ev.models}
    size = {cid: f"{v['chunking'].max_tokens}/{v['chunking'].overlap_tokens}" for cid, v in configs.items()}

    def label(key: tuple[str, str, str, str]) -> str:
        model_id, prefix, config_id, vectors = key
        return (
            f"{name[model_id]} {size[config_id]}"
            + (" +prefix" if prefix else "")
            + (" gold" if vectors == "gold" else "")
        )

    def series(key: tuple[str, str, str, str], metric: str) -> list[float]:
        return [r[metric] for r in rankings[key]]

    out["results"] = {
        label(k): {
            "model_id": k[0],
            "query_prefix": k[1],
            "config_id": k[2],
            "vectors": k[3],
            f"recall@{ev.recall_k}": vars(bootstrap(series(k, "hit"), draws)),
            f"mrr@{ev.mrr_k}": vars(bootstrap(series(k, "rr"), draws)),
        }
        for k in rankings
    }
    primary = {m.model_id: m.query_prefix for m in ev.models}
    big, small = (c.config_id for c in config.chunk_grid)
    pairs = []
    for m in ev.models:  # chunk size, within each model
        pairs.append(
            (
                (m.model_id, primary[m.model_id], big, "local"),
                (m.model_id, primary[m.model_id], small, "local"),
            )
        )
    a, b = ev.models[0], ev.models[1]
    for cid in (big, small):  # model, within each size
        pairs.append(
            ((b.model_id, primary[b.model_id], cid, "local"), (a.model_id, primary[a.model_id], cid, "local"))
        )
    for m in ev.models:  # query prefix, within each size
        if m.query_prefix:
            for cid in (big, small):
                pairs.append(((m.model_id, m.query_prefix, cid, "local"), (m.model_id, "", cid, "local")))
    gold_key = (config.embedding.model_id, config.embedding.query_prefix, config.chunk.config_id, "gold")
    pairs.append((gold_key, (gold_key[0], gold_key[1], gold_key[2], "local")))
    out["differences"] = [
        {
            "a": label(x),
            "b": label(y),
            f"recall@{ev.recall_k}": vars(paired_difference(series(x, "hit"), series(y, "hit"), draws)),
            f"mrr@{ev.mrr_k}": vars(paired_difference(series(x, "rr"), series(y, "rr"), draws)),
        }
        for x, y in pairs
    ]
    out["per_item"] = {label(k): {r["item_id"]: r["rank"] for r in v} for k, v in rankings.items()}
    out["environment"] = {
        "machine": platform.machine(),
        "python": platform.python_version(),
        "packages": {p: importlib.metadata.version(p) for p, _ in config.embedding.packages},
        "numpy": np.__version__,
    }
    run_id = f"{args.label}-{uuid.uuid4().hex[:8]}"
    out["eval_run_id"] = run_id
    out["finished_utc"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    out_path.write_text(redactor.dump(out))  # the results are saved before the table write is tried
    if args.write_table:
        out["table"] = write_table(
            w, warehouse, config, args.table, es, set_sha, run_id, head, rankings, configs
        )
        out_path.write_text(redactor.dump(out))
    for lab, r in out["results"].items():
        rk, mk = r[f"recall@{ev.recall_k}"], r[f"mrr@{ev.mrr_k}"]
        print(
            f"{lab:40s} recall@{ev.recall_k} {rk['mean']:.2f} [{rk['low']:.2f}, {rk['high']:.2f}]  "
            f"MRR@{ev.mrr_k} {mk['mean']:.3f} [{mk['low']:.3f}, {mk['high']:.3f}]"
        )


def gold_check(
    w: WorkspaceClient, warehouse: str, config: Config, info: dict[str, Any], local: np.ndarray
) -> dict[str, Any]:
    """Gold's vectors for the published config's chunks, and their cosine with the vectors embedded here."""
    rows = run_statement(
        w, warehouse, "SELECT chunk_id, embedding FROM IDENTIFIER(:table)", {"table": config.gold_table or ""}
    )["rows"]
    gold = {r[0]: [float(x) for x in json.loads(r[1])] for r in rows}
    ids = [c.chunk_id for c in info["chunks"]]
    if set(gold) != set(ids):
        raise SystemExit(f"gold holds {len(gold)} chunks, not exactly the {len(ids)} of the published config")
    gv = np.asarray([gold[i] for i in ids])
    cos = np.sum(gv * local, axis=1) / (np.linalg.norm(gv, axis=1) * np.linalg.norm(local, axis=1))
    return {
        "rows": len(gold),
        "min_cosine": float(cos.min()),
        "mean_cosine": float(cos.mean()),
        "_vectors": gv,
    }


def write_table(
    w: WorkspaceClient,
    warehouse: str,
    config: Config,
    table_name: str,
    es: EvalSet,
    set_sha: str,
    run_id: str,
    head: str,
    rankings: dict[tuple[str, str, str, str], list[dict[str, Any]]],
    configs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """One row per item and ranking into eval_results: JSON lines uploaded to the artifacts Volume, then a
    MERGE on the row's key, read with read_files."""
    ev = config.eval
    at = dt.datetime.now(dt.UTC).isoformat()
    lines = []
    for (model_id, prefix, config_id, vectors), rows in rankings.items():
        chunking = configs[config_id]["chunking"]
        for r in rows:
            lines.append(
                json.dumps(
                    {
                        "eval_run_id": run_id,
                        "eval_set_version": es.version,
                        "eval_set_sha256": set_sha,
                        "item_id": r["item_id"],
                        "model_id": model_id,
                        "query_prefix": prefix,
                        "config_id": config_id,
                        "max_tokens": chunking.max_tokens,
                        "overlap_tokens": chunking.overlap_tokens,
                        "version_tag": es.tag,
                        "vectors": vectors,
                        "rank_first_relevant": r["rank"],
                        "hit_at_k": r["hit"],
                        "rr_at_k": r["rr"],
                        "recall_k": ev.recall_k,
                        "mrr_k": ev.mrr_k,
                        "top_chunk_ids": r["top"],
                        "relevant_chunk_ids": r["relevant"],
                        "evaluated_at": at,
                        "code_commit": head,
                    }
                )
            )
    path = f"{config.volume(ARTIFACTS)}/eval/{run_id}.json"
    w.files.upload(path, io.BytesIO(("\n".join(lines) + "\n").encode()), overwrite=False)
    table = config.table(table_name)
    run_statement(
        w,
        warehouse,
        f"CREATE TABLE IF NOT EXISTS IDENTIFIER(:table) ({eval_results_ddl()}) "
        "COMMENT 'One row per eval item and ranking (model, prefix, chunk config, vectors). See D-015.'",
        {"table": table},
    )
    on = " AND ".join(f"t.{k} = s.{k}" for k in EVAL_RESULTS_KEY)
    merged = run_statement(
        w,
        warehouse,
        f"MERGE INTO IDENTIFIER(:table) AS t USING (SELECT * FROM read_files(:path, format => 'json', "
        f"schema => '{eval_results_ddl(nullable=True)}')) AS s ON {on} WHEN NOT MATCHED THEN INSERT *",
        {"table": table, "path": path},
    )
    back = run_statement(
        w,
        warehouse,
        "SELECT count(*) FROM IDENTIFIER(:table) WHERE eval_run_id = :run",
        {"table": table, "run": run_id},
    )["rows"][0][0]
    if int(back) != len(lines):
        raise SystemExit(f"eval_results holds {back} rows of {run_id}, {len(lines)} were written")
    return {
        "table": table,
        "rows": len(lines),
        "merge": dict(zip(merged["columns"], merged["rows"][0], strict=True)),
        "file": path,
    }


if __name__ == "__main__":
    main()
