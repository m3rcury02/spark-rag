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
  Delta Sync index with self-managed embeddings. **Status: proposed, pending the
  workspace smoke test** (`notebooks/00_workspace_smoke_test.py`, see D-001).
- Python 3.12 (matches serverless environment versions 3–6). Managed with `uv`.
- Embeddings: open-source sentence-transformers models, computed by us.
- Deployment: Databricks Asset Bundles (dev/prod targets). CI: GitHub Actions.

## Code conventions

- `src/docs_rag/` holds **pure-Python core logic** (fetch, hash, parse, chunk,
  diff, DQ rules, metrics). It must not import pyspark, so unit tests run in CI
  without a JVM or a workspace.
- Spark/Databricks code (Delta reads/writes, MERGE, AI Search calls) lives in thin
  adapter modules and notebooks/jobs that call the core.
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
```

## Next session: pick up here

1. Confirm Databricks auth works: `DATABRICKS_HOST` is an env var. `DATABRICKS_TOKEN` is either an env var
   or a network secret that the proxy injects. Check with `databricks current-user me` (install the CLI
   first) or the Python SDK. Never print the token.
2. Run `notebooks/00_workspace_smoke_test.py` in the owner's workspace as a one-off serverless run.
   Import it, submit the run, and fetch the output. Save the summary JSON to `runs/m0_smoke_test.json`.
3. Using that result, update D-001 to Accepted (or switch to the pgvector fallback) and report to the owner.
4. Do not start M1 until the owner says so.

## Milestones (status)

- [x] M0 — Platform research, scaffold, workspace smoke test (awaiting owner's run)
- [ ] M1 — Bronze ingestion: fetch tag → `bronze_docs`, content hash, idempotent MERGE, tests, CI
- [ ] M2 — Silver: Markdown cleaning + section-aware chunking, stable chunk IDs, tests
- [ ] M3 — Change detection (doc + chunk level) and run log; v1 → v2 → re-run
- [ ] M4 — DQ checks as a gate between silver and gold
- [ ] M5 — Embeddings + AI Search index sync (only changed chunks embedded)
- [ ] M6 — Eval set (50 Q/A, owner reviews) + recall@5/MRR, 2 chunk sizes × 2 models
- [ ] M7 — Asset Bundles (dev/prod), Jobs, GitHub Actions
- [ ] M8 — README: Mermaid diagram, how to run, results table, limitations
- [ ] M9 — (optional) FastAPI QA endpoint with citations
