# DECISIONS.md

Log of significant choices. Format: context → options → decision → why → consequences.
Status is one of **Proposed** (needs owner sign-off or evidence), **Accepted**, **Superseded**.
Facts from docs carry the URL and the "last updated" date we read on that page; facts we
measured carry the exact command.

---

## D-001 — Vector index platform

- **Date:** 2026-10-14 · **Status:** Accepted 2026-10-15 (workspace smoke test passed, no fallback criterion met)

**Context.** The brief says to prefer Databricks Vector Search synced from a Delta table if the
Free Edition workspace supports it, and to fall back to Postgres + pgvector in Docker otherwise.

**What the docs say (read 2026-10-14).** Links updated 2026-10-15: the `/vector-search/` pages now
301-redirect to `/ai-search/`.

| Fact | Source |
|---|---|
| Vector Search was renamed **AI Search**. The new Python client is `databricks-ai-search` (v0.78 on PyPI). Its REST paths are still `/api/2.0/vector-search/...` | [AI Search overview](https://docs.databricks.com/aws/en/ai-search/ai-search) (updated 2026-09-14); SDK source we inspected |
| Free Edition allows **one AI Search endpoint with one search unit**. **Direct Vector Access indexes are not supported**, so Delta Sync is the only index type available | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (updated 2026-09-29) |
| Serverless only, max 5 concurrent job tasks, one 2X-Small SQL warehouse, no GPU serving, limited model-serving endpoints | same |
| **Outbound internet is restricted to an unpublished allowlist of "trusted domains."** Community reports say pypi.org works, the allowlist differs by region, and github.com was reachable in one report after identity verification. Nobody reports on huggingface.co | same, plus [community thread](https://community.databricks.com/t5/databricks-free-edition-help/whitelist-for-outbound-network-access/td-p/146591) and [a GitHub issue](https://github.com/VirtueMe/stavangerparking/issues/121) |
| Under the fair-use quota, compute is shut off for the rest of the day (or month) when exceeded. Accounts may be deleted after prolonged inactivity | Free Edition limitations |
| A Delta Sync source table on a standard endpoint needs **Change Data Feed** enabled. The index schema is fixed at creation | [Create an index](https://docs.databricks.com/aws/en/ai-search/create-ai-search) (updated 2026-10-09) |
| On standard endpoints, triggered and continuous syncs are both incremental: "only data that has changed since the last sync is processed" | Create an index (read 2026-10-15) |
| HNSW with L2 distance. Cosine similarity requires normalized embeddings. Hybrid (BM25 + vector, RRF) search is available | AI Search overview |
| The docs do not say how deletes in the source table reach the index | Create an index. **We measured it: see the evidence below** |

**Options.**
1. **AI Search Delta Sync index, self-managed embeddings, standard endpoint, TRIGGERED sync.**
   We compute the embeddings and store them in a Delta table with CDF on. The index syncs from that table.
2. AI Search Delta Sync with Databricks-computed embeddings. This is available: the workspace lists
   Foundation Model embedding endpoints (`databricks-bge-large-en`, `databricks-gte-large-en`,
   `databricks-qwen3-embedding-0-6b`; `runs/m0_workspace_inventory.json`). It is still the wrong fit. An index
   takes a single `embedding_model_endpoint_name` (`create_delta_sync_index` in `databricks-ai-search` 0.78),
   so comparing two models means two indexes. The brief also asks for open-source models we choose and run
   ourselves, and these endpoints' Free Edition rate limits are undocumented.
3. AI Search Direct Vector Access. **Not available on Free Edition.**
4. Postgres + pgvector in Docker, with a local scheduler.

**Evidence: workspace smoke test, 2026-10-14.** A one-off serverless run of
`notebooks/00_workspace_smoke_test.py` (commit `d36ee94`), submitted with `scripts/m0_submit_smoke_test.py`.
The run took 1505.1 s. Every file below was written by `scripts/m0_collect_evidence.py`, with account
identifiers redacted. Commands:
`uv run --with databricks-sdk==0.152.0 python scripts/m0_submit_smoke_test.py`, then
`uv run --with databricks-sdk==0.152.0 --with databricks-ai-search==0.78 python scripts/m0_collect_evidence.py --run-id 1057608993563169`.

| Question | Result | Evidence (`runs/`) |
|---|---|---|
| Is this Free Edition? | Consistent with it. The Premium-only setting `enableIpAccessLists` is refused as "not available for your pricing tier". The workspace has 0 clusters and one 2X-Small serverless warehouse | `m0_workspace_inventory.json` |
| Outbound network from serverless | All 8 probed hosts answered over HTTPS: pypi.org, files.pythonhosted.org, github.com, api.github.com, codeload.github.com, raw.githubusercontent.com, huggingface.co, download.pytorch.org | `m0_smoke_test.json` → `network` |
| Package install | `%pip install databricks-ai-search==0.78 sentence-transformers==6.1.0` plus a Python restart took 127.1 s. torch was not preinstalled, so pip pulled the CUDA build `2.14.1+cu130` | `install` |
| Embedding models on CPU | Both models downloaded from huggingface.co and loaded. On 256 texts of 189 tokens (batch 32, one pass): all-MiniLM-L6-v2 ran at 10.6 texts/s and bge-small-en-v1.5 at 6.6 texts/s. Hardware: 4 vCPU aarch64, 15.3 GB | `embeddings`, `compute` |
| Endpoint | `docs_rag_endpoint` (STANDARD) reported ONLINE 0.1 s after it was created. The real provisioning happened when the first index was created: the index sat in `PROVISIONING_ENDPOINT` for at least 1043 s | `endpoint`; `m0_smoke_test_cells.txt` cell 12 |
| Delta Sync index (self-managed 384-d vectors, TRIGGERED) | Created and ready in 1138.3 s with 5 rows indexed. The test query's top-1 result was the expected row (a mechanics check, not a quality check) | `delta_sync` |
| Do UPDATE and DELETE propagate? | **Yes.** We ran `DELETE` on c3, a `MERGE` update on c4, then `index.sync()`. The index then held exactly c1, c2, c4 and c5, with c4's new text, and `indexed_row_count` went from 5 to 4. **Confirmed independently** from outside the workspace with `scan()`, which lists every row by primary key. The notebook's own check used ANN search, which can in principle miss a row | `delta_sync`; `m0_index_scan.json` |
| Was it a real incremental sync? | **Yes.** `sync()` started its own pipeline update 11.3 s after the initial update completed, with `full_refresh=false`. It took 65.1 s, of which 49.0 s was start-up before it reached RUNNING. The notebook's 83.0 s "visible after" figure includes its 20 s polling interval | `m0_sync_pipeline.json` |

**Fallback criteria check.** None was met.
- *The index cannot be created or synced on Free Edition.* It was created and it synced.
- *A deleted source row is still in the index after a sync completes.* It was gone (scan).
- *No embedding model can be loaded at all.* Both models loaded straight from huggingface.co.

**Decision.** Option 1.

**Why.**
- It is the only index type that fits Free Edition and keeps the embedding model under our control.
- Deletes and updates reach the index through CDF and a triggered sync, with no hand-written index calls.
  This was measured above, not assumed.
- TRIGGERED sync fits a batch pipeline. CONTINUOUS sync provisions a streaming cluster, which costs
  more on paid tiers.

**What the smoke test does not show.** These risks carry into M3 and M5.
- **Scale.** We tested 5 rows and one sync, so we know nothing yet about sync time at corpus size.
- **Capacity.** The docs say "Index capacity is provisioned based on the source table size at index creation
  time. Starting with a small source table limits how much the index can grow and can result in
  capacity-exhausted errors" ([AI Search overview](https://docs.databricks.com/aws/en/ai-search/ai-search), updated 2026-09-14). The smoke index was created
  from 5 rows, so it must not be grown into the real index.
- **Write patterns not tested.** We tested only `DELETE` and `MERGE ... WHEN MATCHED UPDATE`. We did not test
  `MERGE ... WHEN NOT MATCHED BY SOURCE DELETE`, `CREATE OR REPLACE` or an overwrite of the source table.
  The docs list what `REPLACE` preserves as "the table history and granted privileges, row filters, and
  column masks" ([CREATE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using), updated 2026-09-11). Table properties such as
  `delta.enableChangeDataFeed` are not on that list, so a `CREATE OR REPLACE` might turn CDF off and break the
  sync. That is our inference, and it is untested. Gold writes therefore stay MERGE/DELETE only (CLAUDE.md data conventions). M5 re-tests delete
  propagation with the exact MERGE the pipeline uses.
- **Quota.** The roughly 25-minute run hit no limit, but we cannot observe fair-use consumption:
  `system.billing.usage` has 0 rows (`m0_workspace_inventory.json`). Whether an idle endpoint that holds an
  index counts against the quota is unknown.
- **Row counts.** The sync pipeline's `num_output_rows` is 0 for every update, including the initial 5-row
  sync, so it cannot tell us how many rows changed. The M3 run log must take add/change/delete counts from
  our own MERGE metrics.
- **Throughput.** It was measured once, on the CUDA torch build, with no repeat runs. Treat the figures as an
  order of magnitude.
- **Runtime.** The run got serverless environment `client.2.5` with Python 3.11.10, not 3.12 (see D-005).

**Consequences.**
- Keep `docs_rag_endpoint`, because Free Edition allows only one endpoint. The smoke-test index, table and
  schema can be dropped with notebook cell 6; that is the owner's call.
- Create the production index once, after `gold_chunk_embeddings` holds the full first backfill (v3.5.0),
  so its capacity matches the corpus. If capacity errors still appear, recreate the index rather than trying to
  grow it.
- Call `index.sync()` in the same job right after each gold MERGE, and log the pipeline update id.
  This keeps the CDF gap short. CDF records "are transient and only accessible for a specified retention
  window" ([change data feed](https://docs.databricks.com/aws/en/tables/features/change-data-feed), updated 2026-10-09), so an index that goes a long time without a sync may
  be unable to read the changes it missed. The recovery path is to drop the index and recreate it from the gold
  table.
- Every sync carries about 50 s of fixed start-up (49.0 s measured), so sync once per run, not once per batch.
- One endpoint means the 2×2 eval grid cannot have four live indexes. The eval harness will rank with an
  exact (brute-force) in-memory search over the gold vectors. AI Search serves only the production
  config. Any recall difference between ANN and exact search is noted as a limitation.
- The workspace can be deleted after inactivity, so every reported number needs a committed artifact
  under `runs/`.

---

## D-002 — Corpus versions for the incremental demo

- **Date:** 2026-10-14 · **Status:** Accepted (owner confirmed 2026-10-14)

**Context.** We need a first tag and a later tag. To exercise every code path, the pair must include
added, modified **and deleted** docs.

**Measured** (`git diff --no-renames --name-status <a> <b> -- 'docs/*.md'` on blob-less clones of the
tags; `.md` file counts from `git ls-tree`):

| Transition | .md files (before → after) | Added | Modified | Deleted |
|---|---|---|---|---|
| v3.5.0 → v4.0.0 | 244 → 212 | 19 | 96 | 51 |
| v4.0.0 → v4.0.1 | 212 → 213 | 1 | 4 | 0 |
| v4.0.0 → v4.1.0 | 212 → 225 | 13 | 30 | 0 |
| v4.1.0 → v4.2.0 | 225 → 238 | 13 | 56 | 0 |

No 4.x transition deletes any files. 50 of the 51 deletions in 3.5 → 4.0 are generated
`sql-error-conditions-*` pages.

**Options.** (a) a 4.x pair: realistic, but it never exercises delete. (b) v3.5.0 → v4.0.0:
exercises all three paths, but it touches most of the corpus. (c) a three-run sequence.

**Decision.** (c): **v3.5.0 → v4.0.0 → v4.0.0 again (idempotency) → v4.0.1.**
The 3.5 → 4.0 run proves add/change/delete. The re-run must log 0/0/0. The 4.0.1 patch shows the
common case, where 5 of 213 docs change.

**Consequence.** The table above is a ready-made **test oracle**: the pipeline's doc-level change
counts must equal these git counts. Renames show up as delete + add because we key by path.

---

## D-003 — Where the processing runs

- **Date:** 2026-10-14 · **Status:** Proposed

**Context.** The corpus is small: v4.1.0 has 225 `.md` files totalling 2,853,739 bytes
(`git ls-tree -r -l`).

**Options.** (a) PySpark transformations end to end. (b) Pure-Python core functions (parse, chunk,
hash, diff), run on the driver, with Spark used only for Delta I/O and MERGE. (c) pandas UDFs /
`mapInPandas` that wrap the same pure functions.

**Proposed decision.** (b), keeping the core functions UDF-ready so we can move to (c) if needed.

**Why.** Distributed compute adds nothing at 3 MB. Pure functions are unit-testable in CI with no JVM.
The value Databricks adds here is Delta (MERGE, CDF, time travel), Unity Catalog governance, Jobs, and
the managed index sync, not parallelism. We will say this plainly in the README rather than pretend
this is a big-data workload.

---

## D-004 — Milestone order: DQ before gold, CI from M1

- **Date:** 2026-10-14 · **Status:** Proposed

The brief lists DQ checks after embeddings and CI with deployment. Two changes:
- **DQ runs as a gate between silver and gold (write-audit-publish).** If a check fails after we have
  already written to the index source table, the index ends up half updated. Checks must run on the
  staged change set before any publish.
- **CI runs the unit tests from M1.** It costs nothing, and every later milestone gets a green or red
  signal.

---

## D-005 — Tooling

- **Date:** 2026-10-14 · **Status:** Accepted
- `uv` for environments and lockfile; Python **3.12** (serverless environment versions 3–6 all ship
  3.12.3 per the [serverless env release notes](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/),
  updated 2026-10-12).
- `ruff` (lint + format), `mypy --strict` on `src/`, `pytest`.
- **Measured 2026-10-14: 3.12 is not what a job gets by default.** The M0 smoke test (a notebook task
  submitted with the Jobs API and no environment spec) ran on serverless environment `client.2.5` with
  Python **3.11.10** on aarch64 (`runs/m0_smoke_test.json` → `compute`). That matches
  [environment version 2](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/two)
  ("Python : 3.11.10", updated 2026-09-11). **Consequence:** M7 must pin the serverless environment version
  for every job task and verify it with a run. Until then, Databricks-side code must not rely on
  3.12-only syntax.

---

## Open questions (decide in the milestone noted)

- **M2/M5 — chunk size vs model context.** `all-MiniLM-L6-v2` truncates input at 256 word-pieces.
  `bge-small-en-v1.5` truncates at 512. Any chunk longer than the model's limit is silently truncated,
  which would confound the chunk-size × model comparison. "Oversized chunk" (DQ) should be defined as
  *longer than the embedding model's max sequence length, in that model's tokenizer*.
- **M3 — chunk ID stability.** Fixed-size windows shift when text is inserted upstream. Plan: chunk
  within heading sections and use content-addressed chunk IDs, so an edit only re-embeds chunks in the
  section it touches.
- **M4 — row-count drift threshold.** v3.5.0 → v4.0.0 legitimately removes 32 net docs (−13%). A
  hard-fail drift check needs a threshold plus an explicit, logged override. Otherwise it fires on
  every major release and someone disables it.
- **M6 — relevance labels must not depend on chunking.** Gold answers are labelled as
  (doc path, section) plus an answer span. A retrieved chunk counts as relevant if it overlaps the
  span. Otherwise chunk sizes cannot be compared. With n=50, the 95% CI half-width on recall@5 is
  ±8.3 / ±11.1 / ±13.9 points at recall 0.9 / 0.8 / 0.5 (normal approximation, 1.96·√(p(1−p)/50)),
  so we report bootstrap CIs and paired differences, not just point estimates.
