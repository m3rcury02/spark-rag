# CLAUDE.md — project conventions

Incremental RAG ingestion pipeline over the Apache Spark docs (`docs/*.md` in
apache/spark, Apache 2.0), plus a retrieval evaluation harness. Portfolio project:
small and finished beats big and half-done.

Full requirements: `docs/BRIEF.md`. Design decisions and evidence: `DECISIONS.md`.
Read both before starting any milestone.

## Working agreement (from the project owner)

- Work milestone by milestone. After each one: how to run it, what to measure,
  5 interview-style questions. **Do not start the next milestone until the owner says so.**
- **Never invent metrics.** Every number in README/DECISIONS must come from a run we
  actually did, and its raw output must be saved under `runs/` (committed) so the
  number can be traced. If a number is from a docs page or a `git diff`, say so.
- Log every significant choice in `DECISIONS.md` (options, choice, why, evidence).
- Budget is ~zero. **Flag anything that costs money before using it** (Azure trial
  credit counts as money: it expires and converts to pay-as-you-go).
- Git: **never sign or co-sign commits** — no GPG signing (`commit.gpgsign=false`
  is set locally), no `Co-Authored-By:` or other attribution trailers.
  Commit author: `Gunal Seenivasagan <74795556+m3rcury02@users.noreply.github.com>`. The owner
  chose the GitHub no-reply address on purpose, to keep their personal email out of the public
  history. A fresh container defaults to a different identity, so set these with
  `git config --local` before the first commit (user.name, user.email, commit.gpgsign=false).
- Challenge weak reasoning, including the owner's. Prefer truth over agreement.

## Stack

- Platform: Databricks Free Edition (serverless only), Unity Catalog, Delta.
  Vector index: Databricks **AI Search** (renamed from Vector Search in 2026) —
  Delta Sync index `gold_chunk_index` over `gold_chunk_embeddings`, self-managed embeddings, on the single
  endpoint `docs_rag_endpoint`. Both exist only while needed (M5 created and deleted them on 2026-10-20, D-014).
  **Status: accepted 2026-10-15** after the workspace smoke test (D-001, evidence in `runs/m0_*`).
- Python 3.12, managed with `uv`. Every Databricks job pins **serverless environment version 4** (Python
  3.12.3) and asserts it at start-up; with no spec a job got env 2 / Python 3.11 (D-005). The package version
  comes from git (hatch-vcs), so serverless never runs a cached older wheel.
- Embeddings: open-source sentence-transformers models, computed by us: all-MiniLM-L6-v2 at a pinned revision,
  CPU torch pinned by sha256, checked against a shipped canary before gold is touched (D-014).
- Deployment: Databricks Asset Bundles (dev/prod targets). CI: GitHub Actions.

## Code conventions

- `src/docs_rag/` holds **pure-Python core logic** (fetch, hash, parse, chunk,
  diff, DQ rules, metrics). It must not import pyspark, so unit tests run in CI
  without a JVM or a workspace.
- Spark/Databricks code (Delta reads/writes, MERGE, AI Search calls) lives in thin
  adapter modules and notebooks/jobs that call the core: `jobs/` holds Databricks job scripts,
  `scripts/` holds local tooling that runs them (`run_job.py`, `run_sql.py`, verification).
- Everything written to `runs/` goes through `scripts/redact.py` (the repo is public; Databricks
  error messages name the owner's email). Run tooling refuses to run on uncommitted code.
- Type hints on every function; `mypy --strict` on `src/`. `ruff` for lint+format.
- Frozen dataclasses for records crossing layer boundaries.
- Tests in `tests/`, pytest, no network. Use small fixture docs, not the live corpus.
- Config (chunk size, overlap, model, tags, table names) comes from one typed
  config object, never hard-coded in functions.

## Data conventions

- Medallion layout in one UC schema per target: `bronze_docs`, `silver_chunks`,
  `gold_chunk_embeddings` (index source), plus `ops_run_log`, `ops_dq_results`,
  `eval_results`.
- Catalog `workspace` (Free Edition default). Schemas `docs_rag_dev` / `docs_rag_prod`.
- Every write is idempotent (MERGE on natural keys). Re-running a step on the same
  input must change nothing and must log zero adds/changes/deletes.
- A failed DQ check stops the run **before** anything is published to the index
  (write-audit-publish).

## Commands

```bash
uv sync                      # install deps
uv run pytest                # unit tests
uv run ruff check . && uv run ruff format --check .
uv run mypy src

# M0 smoke test (needs DATABRICKS_HOST / DATABRICKS_TOKEN): submit, collect evidence into runs/, clean up.
# The smoke index was deleted on 2026-10-15, so collect_evidence only works after a fresh submit.
uv run --with databricks-sdk==0.152.0 python scripts/m0_submit_smoke_test.py
uv run --with databricks-sdk==0.152.0 --with databricks-ai-search==0.78 \
    python scripts/m0_collect_evidence.py --run-id <run id>
uv run --with databricks-sdk==0.152.0 python scripts/m0_cleanup.py

# M1 bronze (needs the same credentials). Each writes runs/<label>.json.
uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
    --script jobs/bronze_ingest.py --label <label> -- --tag v3.5.0
uv run --with databricks-sdk==0.152.0 python scripts/run_sql.py --label <label> --sql "<statement>"
uv run --with databricks-sdk==0.152.0 python scripts/m1_verify_bronze.py \
    --label <label> --tags v3.5.0 v4.0.0 v4.0.1

# M2 silver (same credentials): chunk one bronze tag under one config of Config.chunk_grid (254/32 is the
# default; also run --max-tokens 128 --overlap-tokens 16); then compare silver with a local chunking of git.
uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
    --script jobs/silver_chunk.py --label <label> -- --tag v3.5.0 [--max-tokens 128 --overlap-tokens 16]
uv run --with databricks-sdk==0.152.0 python scripts/m2_verify_silver.py \
    --label <label> --tags v3.5.0 v4.0.0 v4.0.1 --job-runs runs/m2_d[1-9]*.json
# M2 local evidence (git only; Databricks just for redaction): ID stability, corpus numbers, ablation.
uv run --with databricks-sdk==0.152.0 python scripts/m2_stability.py --label <label> --tags v3.5.0 v4.0.0 v4.0.1
# Token counts vs the models' own preprocessing (CPU torch from the PyTorch index; ~1 min).
uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
    --with torch==2.14.1 --index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match \
    python scripts/m2_tokenizer_parity.py --label <label> --tags v3.5.0 v4.0.0 v4.0.1
# M3 change detection + M4 DQ gate (same credentials): diff a tag against the head of the committed chain in
# ops_run_log, for Config.chunk, then gate it (ops_dq_results) before committing; needs bronze and silver of the
# tag. The first run of a sink needs --initial; going back a release needs --allow-backwards; a target under
# another chunk config than its baseline (a cleaner/chunker version bump) needs --allow-config-change (D-013).
# Drift over its threshold (5% patch, 10% minor/major) fails the run; to accept it, run again naming that
# failed run (D-012 gate v2). Then check every logged run against git, D-002 and runs/m2_e, and every gated
# run's DQ rows against git (configs today's code cannot reproduce are re-chunked by scripts/oldcode.py).
uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
    --script jobs/change_detect.py --label <label> -- --tag v4.0.1 [--initial] [--allow-backwards] \
    [--allow-config-change] [--accept-drift "<reason>" --accept-drift-of <failed run_id>]
# The verifiers read one chain (--sink): 'none' for M3/M4's runs, gold_chunk_embeddings for M5's.
uv run --with databricks-sdk==0.152.0 python scripts/m3_verify_changes.py --label <label> --sink none \
    --job-runs runs/m3_*_detect_*.json runs/m4_*_detect_*.json runs/m4x_*_detect_*.json
uv run --with databricks-sdk==0.152.0 python scripts/m4_verify_gate.py --label <label> --sink none \
    --job-runs runs/m4_*_detect_*.json runs/m4x_*_detect_*.json --pre-gate-runs runs/m3_*_detect_*.json
uv run --with databricks-sdk==0.152.0 python scripts/task_attempts.py --label <label> runs/<run files>
# M4 threshold calibration (git only, ~10 min): every DQ check over every release from v3.0.0 on.
uv run --with databricks-sdk==0.152.0 python scripts/m4_calibrate.py --label <label>
# The real corpus as a golden (git only, ~1 min; also in CI and before every silver job in run_job.py):
uv run python scripts/corpus_fingerprint.py
# After a deliberate cleaner/chunker change: bump CLEANER_VERSION (clean.py) or CHUNKER_VERSION (chunk.py), then
# create the new versions' goldens. Existing goldens are never overwritten.
UPDATE_GOLDEN=1 uv run pytest tests/test_clean_golden.py tests/test_chunk_golden.py
uv run python scripts/corpus_fingerprint.py --update
# M5 gold + index (D-014). change_detect with Config.sink = gold embeds the added chunks and MERGEs gold
# (run_job.py installs the pinned torch/sentence-transformers). The endpoint and the index bill while they
# exist: create them only for a demo, the index after the full backfill, and delete both afterwards.
uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py create-endpoint --label <label>
uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py create-index --label <label>  # ~17 min
uv run --with databricks-sdk==0.152.0 python scripts/run_job.py --script jobs/sync_index.py --label <label>
uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py status --label <label>
uv run --with databricks-sdk==0.152.0 python scripts/m5_index.py delete-index --label <label>
uv run --with databricks-sdk==0.152.0 python scripts/delete_endpoint.py --endpoint docs_rag_endpoint --label <label>
# Verify gold against git (time travel per run, kept rows not re-embedded, vectors in SQL, a local re-embedding
# sample; --index also scans the index), and the gold chain with the M3/M4 verifiers.
uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
    --with torch==2.14.1+cpu --index https://download.pytorch.org/whl/cpu \
    --index-strategy unsafe-best-match python scripts/m5_verify_gold.py --label <label> [--index]
uv run --with databricks-sdk==0.152.0 python scripts/m4_verify_gate.py --label <label> \
    --sink gold_chunk_embeddings --job-runs runs/m5_*_detect_*.json
uv run --with databricks-sdk==0.152.0 python scripts/m3_verify_changes.py --label <label> \
    --sink gold_chunk_embeddings --job-runs runs/m5_*_detect_*.json
# The embedding canary (one file today, MiniLM; never overwritten): scripts/m5_canary.py, same --with flags.
```

## Next session: pick up here

M5 is complete (D-014; report given to the owner on 2026-10-20) on branch `m5-gold-index` (not yet merged
into `main`; the owner decides). `Config.sink` is `gold_chunk_embeddings`: `jobs/change_detect.py` checks gold
holds the chain head, embeds only the added chunks (canary first), logs five vector checks as a second DQ insert,
MERGEs inserts and deletes (rows never change), checks gold equals the target, then commits. `jobs/sync_index.py`
syncs the index from a committed state and proves it equal by key scan. The gold chain has 4 committed runs:
v3.5.0 (5,567 embedded) → v4.0.0 (drift accepted, 1,866 embedded) → v4.0.0 (0) → v4.0.1 (21 embedded); gold
holds 5,436 rows at version 4. `ops_run_log` has 53 columns (`runs/m5_b` added six). The chain for sink `none`
(15 runs, M3/M4) is unchanged. Evidence: `runs/m5_*`, verified by `m5_v1` (42/42, incl. the index scan),
`m5_v2`/`m5_v2b` (379/379) and `m5_v3b` (85/85; `m5_v3` failed on a verifier defect, kept as history; `m2_e`
has no cleaner-2 numbers, so no `m2_e` comparison applies to this chain). The index and the
endpoint were deleted afterwards (`m5_m`, `m5_n`); gold stays. Do not re-run the `m3_*`, `m4_*`, `m4x_*` or
`m5_*` labels.

**Do not start M6 until the owner says so.** Open items:
- Read the DBU the M5 window used (endpoint 18:26–19:09 UTC on 2026-10-20) from `system.billing.usage` once it
  has rows (`m5_p` found none yet); the m2_d0d query works.
- Untested: the manual repair after a failure between a gold MERGE and its committed row (D-014, decision 5).
- Still open (D-012, D-013): the override's reason test is lexical and nothing records who overrode; gains are
  not capped; the checks are structural (a leftover-markup check is a cheap next one); positions of a stored
  baseline are pinned by nothing and no verifier checks `chunks_kept_relocated`; refusals before the gate are
  evidenced only by the job's own error messages.
- For M6: the 2×2 grid (254/32 and 128/16 × MiniLM and bge-small) cannot have four live indexes (one endpoint,
  quota): rank exactly over gold-like tables, or locally. Silver holds both sizes under cleaner 2; bge needs its
  own canary (revision `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`), which first needs one canary file per model
  (today `m5_canary.py` and `load_canary` handle one), and its query prefix decided. Anything that reads the live
  index should run `sync_index` first: a committed head does not mean the index caught up, and the sync job
  writes no table row (D-014, decision 4).
- Unit-tested only (added after the demo runs, `860e3e1`): a failing canary logged alone as its DQ row; the
  failed-index check in the two sync waits that lacked it.
- Never read "we wrote" from a gold version bump: Databricks commits its own OPTIMIZE/VACUUM (gold v3, D-010).

## Milestones (status)

- [x] M0 — Platform research, scaffold, workspace smoke test (passed 2026-10-14; D-001 accepted)
- [x] M1 — Bronze ingestion: fetch tag → `bronze_docs`, content hash, idempotent MERGE, tests, CI (2026-10-15; D-006, D-007)
- [x] M2 — Silver: Markdown cleaning + section-aware chunking, stable chunk IDs, tests (2026-10-19; D-008, D-009, D-010)
- [x] M3 — Change detection (doc + chunk level) and run log; v1 → v2 → re-run (2026-10-20; D-011)
- [x] M4 — DQ checks as a gate between silver and gold (2026-10-20; D-012; follow-ups D-012 v2, D-013)
- [x] M5 — Embeddings + AI Search index sync (only changed chunks embedded) (2026-10-20; D-014)
- [ ] M6 — Eval set (50 Q/A, owner reviews) + recall@5/MRR, 2 chunk sizes × 2 models
- [ ] M7 — Asset Bundles (dev/prod), Jobs, GitHub Actions
- [ ] M8 — README: Mermaid diagram, how to run, results table, limitations
- [ ] M9 — (optional) FastAPI QA endpoint with citations
