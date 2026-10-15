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
  Delta Sync index with self-managed embeddings, on the single endpoint `docs_rag_endpoint`.
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

# M2 silver (same credentials): chunk one bronze tag; then compare silver with a local chunking of git.
uv run --with databricks-sdk==0.152.0 python scripts/run_job.py \
    --script jobs/silver_chunk.py --label <label> -- --tag v3.5.0
uv run --with databricks-sdk==0.152.0 python scripts/m2_verify_silver.py \
    --label <label> --tags v3.5.0 v4.0.0 v4.0.1
# M2 local evidence (git only; Databricks just for redaction): ID stability, corpus numbers, ablation.
uv run --with databricks-sdk==0.152.0 python scripts/m2_stability.py --label <label> --tags v3.5.0 v4.0.0 v4.0.1
# Token counts vs the models' own preprocessing (CPU torch from the PyTorch index; ~1 min).
uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
    --with torch==2.14.1 --index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match \
    python scripts/m2_tokenizer_parity.py --label <label> --tags v3.5.0 v4.0.0 v4.0.1
# After a deliberate chunker change: bump CHUNKER_VERSION in src/docs_rag/chunk.py, then
UPDATE_GOLDEN=1 uv run pytest tests/test_chunk_golden.py
```

## Next session: pick up here

M1 is complete on branch `m1-bronze` (D-006, D-007, evidence in `runs/m1_*`). `workspace.docs_rag_dev.bronze_docs`
holds v3.5.0 (244 docs), v4.0.0 (212) and v4.0.1 (213). **Wait for the owner to say "start M2" before doing
any M2 work.** Open items:
- Owner decides whether to merge `m1-bronze` into `main`.
- Remote branches `m0-smoke-test` (merged) and `claude/modest-archimedes-mp5zx3` (stale) are still on GitHub:
  the owner approved deleting them, but the session's permission classifier blocked `git push --delete`.
  The owner can delete them in the GitHub UI.
- Re-probe `system.billing.usage` more than 24 h after a run and save the output under `runs/` (D-001 Quota).
- Keep `docs_rag_endpoint` (ONLINE, 0 indexes): Free Edition allows only one endpoint.
- Carry into M2: chunk IDs must be stable across tags (content-addressed within heading sections, see Open
  questions). Silver reads bronze per (version_tag, commit_sha).
- Carry into M3: the diff baseline is the last *published* (version_tag, commit_sha), never "latest in bronze"
  (D-006). The run log takes add/change/delete counts from our own MERGE metrics; the sync pipeline's row
  metrics are always 0 (D-001). On Databricks a no-op MERGE still writes an empty table version (D-006 run b).
- Carry into M5: create the real index only after the full gold backfill, because capacity is sized from the
  source table at creation and a small start limits growth. Re-test insert, update and delete with the real
  MERGE. Sync right after each MERGE and wait for the update to complete ("ready" is not "synced") (D-001).

## Milestones (status)

- [x] M0 — Platform research, scaffold, workspace smoke test (passed 2026-10-14; D-001 accepted)
- [x] M1 — Bronze ingestion: fetch tag → `bronze_docs`, content hash, idempotent MERGE, tests, CI (2026-10-15; D-006, D-007)
- [ ] M2 — Silver: Markdown cleaning + section-aware chunking, stable chunk IDs, tests
- [ ] M3 — Change detection (doc + chunk level) and run log; v1 → v2 → re-run
- [ ] M4 — DQ checks as a gate between silver and gold
- [ ] M5 — Embeddings + AI Search index sync (only changed chunks embedded)
- [ ] M6 — Eval set (50 Q/A, owner reviews) + recall@5/MRR, 2 chunk sizes × 2 models
- [ ] M7 — Asset Bundles (dev/prod), Jobs, GitHub Actions
- [ ] M8 — README: Mermaid diagram, how to run, results table, limitations
- [ ] M9 — (optional) FastAPI QA endpoint with citations
