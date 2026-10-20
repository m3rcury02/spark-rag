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
  Delta Sync index with self-managed embeddings, on the single endpoint `docs_rag_endpoint` (deleted while idle
  on 2026-10-19; M5 creates it again).
  **Status: accepted 2026-10-15** after the workspace smoke test (D-001, evidence in `runs/m0_*`).
- Python 3.12, managed with `uv`. Every Databricks job pins **serverless environment version 4** (Python
  3.12.3) and asserts it at start-up; with no spec a job got env 2 / Python 3.11 (D-005). The package version
  comes from git (hatch-vcs), so serverless never runs a cached older wheel.
- Embeddings: open-source sentence-transformers models, computed by us.
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
# tag. The first run of a sink needs --initial; going back a release needs --allow-backwards; drift over 10% needs
# --accept-drift "<reason>" (D-012). Then check every logged run against git, D-002 and runs/m2_e, and every
# gated run's DQ rows against git.
uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
    --script jobs/change_detect.py --label <label> -- --tag v4.0.1 [--initial] [--allow-backwards] \
    [--accept-drift "<reason>"]
uv run --with databricks-sdk==0.152.0 python scripts/m3_verify_changes.py \
    --label <label> --job-runs runs/m3_*_detect_*.json runs/m4_*_detect_*.json
uv run --with databricks-sdk==0.152.0 python scripts/m4_verify_gate.py \
    --label <label> --job-runs runs/m4_*_detect_*.json --pre-gate-runs runs/m3_*_detect_*.json
uv run --with databricks-sdk==0.152.0 python scripts/task_attempts.py --label <label> runs/<run files>
# M4 threshold calibration (git only, ~10 min): every DQ check over every release from v3.0.0 on.
uv run --with databricks-sdk==0.152.0 python scripts/m4_calibrate.py --label <label>
# The real corpus as a golden (git only, ~1 min; also in CI and before every silver job in run_job.py):
uv run python scripts/corpus_fingerprint.py
# After a deliberate cleaner/chunker change: bump CLEANER_VERSION (clean.py) or CHUNKER_VERSION (chunk.py), then
# create the new versions' goldens. Existing goldens are never overwritten.
UPDATE_GOLDEN=1 uv run pytest tests/test_clean_golden.py tests/test_chunk_golden.py
uv run python scripts/corpus_fingerprint.py --update
```

## Next session: pick up here

M4 is complete on branch `m4-dq-gate` (D-012; M4 report given to the owner on 2026-10-20). M3 is merged into
`main`. The DQ gate runs inside `jobs/change_detect.py` before the committed row; `ops_dq_results` (append-only)
holds one row per check per gated run, and `ops_run_log` has a new last column `dq_override` (added by `runs/m4_b`).
The chain for sink `none` now has 9 committed runs and ends at v4.0.1 again (M4 demo: rollback to v3.5.0 and
forward to v4.0.0, both with `--accept-drift`). Evidence: `runs/m4_a`..`m4_l`, verified by `m4_v` (501/501) and
`m4_w` (184/184). Do not re-run the `m3_*` or `m4_*` labels.

**Do not start M5 until the owner says so.** Open items:
- Owner decides whether to merge `m4-dq-gate` into `main`. Remote branches `m0-smoke-test`, `m1-bronze`,
  `m2-silver`, `m3-change-detection` (all merged) and `claude/modest-archimedes-mp5zx3` (stale) are still on
  GitHub: the session's git proxy refuses branch deletion (HTTP 403), so the owner deletes them in the GitHub UI.
- `docs_rag_endpoint` was deleted on 2026-10-19 05:26Z (idle, 96 DBU/day; D-001). Re-probe
  `system.billing.usage` after 2026-10-20 05:26Z to confirm the endpoint's usage stopped, and save the output
  under `runs/`.
- Owner's call (D-012, Open questions): the drift thresholds. Built: one 10% threshold (fires on 3 of 41 release
  transitions: no patch, 2 of 7 minor, the one major). Alternatives on the same calibration: patch 5% / feature
  10% (same 3 firings, tighter on patches), patch 5% / feature 20% (no firings), or drop/relax `drift_docs`.
- Before M5 (D-012 Consequences): a CLEANER/CHUNKER version bump cannot cross an existing chain (the baseline's
  config_id leaves `Config.chunk_grid`). Either make known cleaner fixes (the lone tab-label chunks) while only
  the `none` chain exists, or build a logged re-baseline path (verify the baseline from stored silver rows and
  its logged fingerprint). Optional: bind `--accept-drift` to a failed run's logged numbers (plan, then apply).
- For M5's gold writes (from M1/M2): take counts from our own MERGE metrics (the sync pipeline's row metrics are
  always 0, D-001); on Databricks a no-op MERGE still writes an empty table version (D-006 run b), and
  Databricks commits its own `OPTIMIZE` versions (D-010): never read "we wrote" from a version bump.
- Carry into M5: set `Config.sink` to the gold table: a new chain, so the first M5 run is an explicit `--initial`
  load into an empty gold, and its drift is not applicable (D-012: decide whether to compare it with the `none`
  chain's head). Order (D-012): chunk checks → embed to staging → vector checks (dimension, no NaN, norm) logged
  like the others → gold MERGE with the `GatePassed` → sync and wait → committed row; define recovery for a
  failure between the MERGE and the committed row. Gold holds no positions (they come from silver; kept chunks
  move, D-011). Create the endpoint again first (~20 min of provisioning) and delete it when the index is not
  needed (idle it bills 4 DBU/h even with no index, against the docs, and likely drains the daily quota: D-001).
  Create the real index only after the full gold backfill, because capacity is sized from the source table at
  creation and a small start limits growth. Re-test insert, update and delete with the real MERGE. Sync right
  after each MERGE and wait for the update to complete ("ready" is not "synced") (D-001).

## Milestones (status)

- [x] M0 — Platform research, scaffold, workspace smoke test (passed 2026-10-14; D-001 accepted)
- [x] M1 — Bronze ingestion: fetch tag → `bronze_docs`, content hash, idempotent MERGE, tests, CI (2026-10-15; D-006, D-007)
- [x] M2 — Silver: Markdown cleaning + section-aware chunking, stable chunk IDs, tests (2026-10-19; D-008, D-009, D-010)
- [x] M3 — Change detection (doc + chunk level) and run log; v1 → v2 → re-run (2026-10-20; D-011)
- [x] M4 — DQ checks as a gate between silver and gold (2026-10-20; D-012)
- [ ] M5 — Embeddings + AI Search index sync (only changed chunks embedded)
- [ ] M6 — Eval set (50 Q/A, owner reviews) + recall@5/MRR, 2 chunk sizes × 2 models
- [ ] M7 — Asset Bundles (dev/prod), Jobs, GitHub Actions
- [ ] M8 — README: Mermaid diagram, how to run, results table, limitations
- [ ] M9 — (optional) FastAPI QA endpoint with citations
