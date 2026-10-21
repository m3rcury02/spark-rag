# DECISIONS.md

Log of significant choices. Format: context → options → decision → why → consequences.
Status is one of **Proposed** (needs owner sign-off or evidence), **Accepted**, **Superseded**.
Facts from docs carry the URL and the "last updated" date we read on that page; facts we
measured carry the exact command.

---

## D-001 — Vector index platform

- **Date:** 2026-10-14 · **Status:** Accepted 2026-10-15 (workspace smoke test passed, no fallback criterion met;
  the owner confirmed in the UI on 2026-10-15 that the workspace is Free Edition)

**Context.** The brief says to prefer Databricks Vector Search synced from a Delta table if the
Free Edition workspace supports it, and to fall back to Postgres + pgvector in Docker otherwise.

**What the docs say (read 2026-10-14).** Links updated 2026-10-15: the `/vector-search/` pages now
301-redirect to `/ai-search/`.

| Fact | Source |
|---|---|
| Vector Search was renamed **AI Search**. The new Python client is `databricks-ai-search` (v0.78 on PyPI). Its REST paths are still `/api/2.0/vector-search/...` | [AI Search overview](https://docs.databricks.com/aws/en/ai-search/ai-search) (updated 2026-09-14); SDK source we inspected |
| Free Edition allows **one AI Search endpoint with one search unit**. **Direct Vector Access indexes are not supported**, so Delta Sync is the only index type available | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (updated 2026-09-29) |
| Serverless only, max 5 concurrent job tasks, one 2X-Small SQL warehouse, no GPU serving, limited model-serving endpoints | same |
| **Outbound internet is restricted to an unpublished allowlist of "trusted domains."** Community reports say pypi.org works and the allowlist differs by region. One report (October 2026) lists "github.com, pypi.org" as "Already reachable" and says other hosts that had failed DNS after identity verification later resolved too. Nobody reports on huggingface.co. **Our own probes: see the evidence table** | same, plus [community thread](https://community.databricks.com/t5/databricks-free-edition-help/whitelist-for-outbound-network-access/td-p/146591) and [a GitHub issue](https://github.com/VirtueMe/stavangerparking/issues/121) (re-read 2026-10-15) |
| Under the fair-use quota, compute is shut off for the rest of the day (or month) when exceeded. Accounts may be deleted after prolonged inactivity | Free Edition limitations |
| A Delta Sync source table on a standard endpoint needs **Change Data Feed** enabled. The index schema is fixed at creation | [Create an index](https://docs.databricks.com/aws/en/ai-search/create-ai-search) (updated 2026-10-09) |
| On standard endpoints, triggered and continuous syncs are both incremental: "only data that has changed since the last sync is processed" | Create an index (read 2026-10-15) |
| HNSW with L2 distance. Cosine similarity requires normalized embeddings. Hybrid (BM25 + vector, RRF) search is available | AI Search overview |
| The docs do not say how deletes in the source table reach the index | Create an index. **We measured it: see the evidence below** |

**Options.**
1. **AI Search Delta Sync index, self-managed embeddings, standard endpoint, TRIGGERED sync.**
   We compute the embeddings and store them in a Delta table with CDF on. The index syncs from that table.
2. AI Search Delta Sync with Databricks-computed (managed) embeddings. The workspace lists Foundation Model
   embedding endpoints (`databricks-bge-large-en`, `databricks-gte-large-en`, `databricks-qwen3-embedding-0-6b`;
   `runs/m0_workspace_inventory.json`). We have not called them, and their Free Edition availability and rate
   limits are undocumented, so this option is untested rather than ruled out.
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
| Is this Free Edition? | **Not proven through the API.** The tier probe refused `enableIpAccessLists` ("not available for your pricing tier"). On AWS that setting needs the Enterprise tier ([IP access lists](https://docs.databricks.com/aws/en/security/network/front-end/ip-access-list-workspace), updated 2026-10-08), so the probe only rules out Enterprise. The rest fits Free Edition but does not prove it: the workspace is on AWS (serverless kernel `amzn2023`), while the brief's only other platform is an Azure free trial; it has 0 clusters and one 2X-Small serverless warehouse. **The owner confirmed Free Edition in the UI on 2026-10-15** (owner's statement, not an API result). | `m0_workspace_inventory.json`; `m0_smoke_test.json` → `compute.platform` |
| Outbound network from serverless | All 8 probed hosts answered over HTTPS: pypi.org, files.pythonhosted.org, github.com, api.github.com, codeload.github.com, raw.githubusercontent.com, huggingface.co, download.pytorch.org | `m0_smoke_test.json` → `network` |
| Package install | `%pip install databricks-ai-search==0.78 sentence-transformers==6.1.0` plus a Python restart took 127.1 s. torch was not preinstalled, so pip pulled the CUDA build `2.14.1+cu130`. pip printed a resolver ERROR about a protobuf conflict with the preinstalled `google-api-core` (cell 2). It did no harm in this run | `install`; `m0_smoke_test_cells.txt` |
| Embedding models on CPU | Both models downloaded from huggingface.co and loaded. On 256 texts of 189 tokens (batch 32, one pass): all-MiniLM-L6-v2 ran at 10.6 texts/s and bge-small-en-v1.5 at 6.6 texts/s. Hardware: 4 vCPU aarch64 with 15.3 GiB (the notebook's `mem_total_gb` key is really GiB). Serverless "does not guarantee a specific CPU architecture" ([dependencies](https://docs.databricks.com/aws/en/compute/serverless/dependencies), updated 2026-09-29), so a later run may land on x86_64 | `embeddings`, `compute` |
| Endpoint | `docs_rag_endpoint` (STANDARD) reported ONLINE 0.1 s after it was created. The first index then waited in `PROVISIONING_ENDPOINT` for at least 1043 s. Whether later index creations wait again is unknown (*partly answered in D-014: the first index on an endpoint created again waited again, 710–741 s; a second index on an already provisioned endpoint is untested*) | `endpoint`; `m0_smoke_test_cells.txt` cell 12 |
| Delta Sync index (self-managed 384-d vectors, TRIGGERED) | Created and reported ready in 1138.3 s with 5 rows indexed. "Ready" came while the initial update was still running (`detailed_state` `ONLINE_TRIGGERED_UPDATE`; that update completed at 23:17:13.758Z). The test query's top-1 result was the expected row (a mechanics check, not a quality check) | `delta_sync`; `m0_sync_pipeline.json` |
| Do UPDATE and DELETE propagate? | **Yes, measured.** We ran `DELETE` on c3, a `MERGE` update on c4, then `index.sync()`. After the sync's update completed, the index held exactly c1, c2, c4 and c5, with c4's new text, and `indexed_row_count` went from 5 to 4. **Confirmed independently** from outside the workspace with `scan()`, which lists every row by primary key; the notebook's own check used ANN search, which could in principle miss a row. The mutations were issued around the time the initial update finished, so these files cannot rule out that its tail applied part of them. Either way, no hand-written index call was needed | `delta_sync`; `m0_index_scan.json` |
| Was `sync()` a separate update? Was it incremental? | **Separate: yes.** `sync()` started its own pipeline update 11.3 s after the initial update completed. It took 65.1 s, of which 49.0 s was start-up before it reached RUNNING; the initial update's start-up was 59.8 s. The notebook's 83.0 s "visible after" figure includes its 20 s polling interval (`time.sleep(20)` in the notebook). **Incremental: not measurable here.** `full_refresh=false` is also set on the initial full snapshot, and `num_output_rows` is 0 for both updates. That only changed rows are processed comes from the docs (see the docs table above) | `m0_sync_pipeline.json`; `m0_smoke_test.json` → `delta_sync` |

**Fallback criteria check.** None was met.
- *The index cannot be created or synced on Free Edition.* It was created and it synced.
- *A deleted source row is still in the index after a sync completes.* It was gone (scan).
- *No embedding model can be loaded at all.* Both models loaded straight from huggingface.co.

**Decision.** Option 1.

**Why.**
- Delta Sync is the only AI Search index type on Free Edition. Between its two embedding modes, option 1
  is the one we measured end to end; option 2 depends on hosted endpoints whose Free Edition availability
  is undocumented and untested.
- Under option 1 the vectors live in our own gold table. The same vectors feed the index, the exact-search
  eval over the 2×2 grid (M6) and our "re-embed only changed chunks" logic (brief item 3, M5). Under option 2
  the platform computes them, and the eval would need a second embedding path through rate-limited endpoints.
- Option 1 lets us pick the models. The brief asks for "a free open-source model (e.g. sentence-transformers)",
  and our planned small models (all-MiniLM-L6-v2, bge-small-en-v1.5) are not among the hosted endpoints.
- Measured: a Delta `DELETE` and a `MERGE` update reach the index after a triggered sync, with no
  hand-written upsert or delete calls. That the sync reads the table's CDF comes from the docs (CDF is
  required); we did not test with CDF off.
- TRIGGERED sync fits a batch pipeline. CONTINUOUS sync provisions a streaming cluster, which costs
  more on paid tiers.

**What the smoke test does not show.** These risks carry into M1, M3 and M5.
- **Scale.** We tested 5 rows and one sync, so we know nothing yet about sync time at corpus size, or
  whether the sync really processes only changed rows.
- **Capacity.** The docs say "Index capacity is provisioned based on the source table size at index creation
  time. Starting with a small source table limits how much the index can grow and can result in
  capacity-exhausted errors" ([AI Search overview](https://docs.databricks.com/aws/en/ai-search/ai-search),
  updated 2026-09-14). The smoke index was created from 5 rows, so it must not be grown into the real index.
- **Write patterns not tested.** All 5 rows existed before the index did, so **inserts after index creation
  (`MERGE ... WHEN NOT MATCHED INSERT`, the path for v4.0.0's 19 added docs) were never exercised.** Nor were
  `MERGE ... WHEN NOT MATCHED BY SOURCE DELETE`, `CREATE OR REPLACE` or an overwrite of the source table.
  The docs list what `REPLACE` preserves as "the table history and granted privileges, row filters, and
  column masks"
  ([CREATE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using),
  updated 2026-09-11). Table properties such as `delta.enableChangeDataFeed` are not on that list, so a
  `CREATE OR REPLACE` might turn CDF off and break the sync. That is our inference, and it is untested. Gold
  writes therefore stay MERGE/DELETE only (CLAUDE.md data conventions). M5 re-tests insert, update and delete
  with the exact MERGE the pipeline uses.
- **"Ready" is not "synced".** The index reported ready while its initial update was still running. Before
  writing to gold or calling `sync()`, and as the success check after it, M5 waits for the pipeline update to
  reach COMPLETED (or for `ONLINE_NO_PENDING_UPDATE`).
- **Quota.** The roughly 25-minute run hit no limit. `system.billing.usage` had 0 rows when probed about
  90 min after the run (`checked_utc` 00:49:17Z in `m0_workspace_inventory.json` against `run_end_utc`
  23:18:53Z in `m0_smoke_test_meta.json`). **Re-probed on 2026-10-18 at 12:17Z** (`runs/m2_b_billing_usage_reprobe.json`):
  Free Edition does record usage there (345 records, 2026-10-14 to 10-18). **The AI Search endpoint is
  billed 4 DBU per hour while idle**: `VECTOR_SEARCH` / `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE` shows exactly
  96 DBU on each full day (10-15, 10-16, 10-17), with 0 indexes since the smoke index was deleted on 10-15.
  Everything else is small beside it: on 10-15, the busiest pipeline day, jobs used 2.30 DBU, SQL 4.84 DBU and
  predictive optimisation 0.31 DBU. Free Edition costs no money, but it has an undisclosed daily quota: "If
  you exceed your quota, your workspace's compute resources will be shut down and unavailable for the rest of
  the day (and in extreme cases, the rest of the month)"
  ([Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations),
  updated 2026-09-29). The amount, the metric, the day boundary and the reset time are not published, and no
  banner, meter or warning is documented (same page; Free Edition has no account console, where the Usage
  page lives).
  **An idle endpoint is billed even with no index**, against the
  [cost guide](https://docs.databricks.com/aws/en/ai-search/cost-management) (read 2026-10-19), which says an
  endpoint stops incurring charges 24 h after its last index is deleted. The smoke index was gone by
  2026-10-15 09:44:39Z (`runs/m0_cleanup.json`), yet the endpoint billed 4.0000 DBU in every hour through
  the 10-19 03:00Z hour (`runs/m2_d0d_billing_hourly.json`), until we deleted the endpoint itself.
  **On 2026-10-19 the workspace refused all compute for a while.** A job submit at about 05:27Z was refused
  (seen in the terminal, not saved: refusals were recorded only from 05:30Z). The saved refusals are a SQL
  statement at 05:30:43Z, "The request could not be processed by the warehouse" (`runs/m2_d0_*`), and a job
  submit at 05:31:49Z, "Triggering new runs for organization <workspace-id> is currently disabled
  temporarily" (`runs/m2_d0b_*`). The next attempt, a SQL probe at 07:57:55Z, succeeded (`runs/m2_d0c_*`),
  and the silver jobs ran from 08:00Z. When compute came back between those times is unknown. The status
  page showed all services operational (observed, not saved). The endpoint had been deleted at 05:26:12Z,
  about 4.5 min before the first saved refusal (`runs/m2_d0b`, `runs/m2_c`). That the deletion caused the
  refusal is unlikely, but it is not ruled out.
  - *What the message means (reported, not documented).* A Databricks employee answering this exact jobs
    message on Free Edition
    ([forum, 2026-10-14](https://community.databricks.com/t5/administration-architecture/can-i-run-jobs-continuously-without-interruption-on-a-permanent/td-p/168057))
    says it "doesn't say why"; the fair-usage quota is the most likely cause, and a block that outlasts the
    daily reset is a workspace or account issue. Users who hit the quota in the UI report an explicit
    "you have hit your free daily limit" message
    ([forum](https://community.databricks.com/t5/administration-architecture/you-have-hit-your-free-daily-limit/td-p/159849)),
    and the same jobs string also hit a paid Azure workspace
    ([Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/5509182/unable-to-start-job-runs-and-start-cluster-in-azur)).
    So the quota is the likely cause, not a proven one.
  - *Which quota model fits (inference from one event, hourly billing in `runs/m2_d0d`).* Counted on the UTC
    day, a job was accepted on 10-15 at 21:25Z after at least 91.2 DBU, yet refused on 10-19 after about
    21.8 DBU (16.0 billed through the 03:00Z hour plus the unbilled endpoint hours). A fixed daily threshold
    on the UTC day fits only if enforcement lags by hours. Counted on the US Pacific day (midnight = 07:00Z
    under PDT), the 10-15 job came after about 63.3 DBU (61.58 billed through the 20:00Z hour) and the
    refusal after about 90.5 DBU (84.73 billed through the 03:00Z hour, plus about 5.7 unbilled), and compute
    was back after the next 07:00Z. A daily quota of roughly 63–90 DBU on the Pacific day fits every
    observation, but only if endpoint DBUs count: without them, nothing but 0.73 DBU of SQL had run in the
    window. That makes the idle endpoint the likely trigger, but it rests on one event.
  - *Consequence.* An idle endpoint (4 DBU/h, about 96 DBU a day) is enough to plausibly exhaust the daily
    quota on its own. M5–M6 create the endpoint only for the hours an index is needed and delete it after;
    `run_job.py` records a refused submit in `runs/`, and the run is retried later by hand.
- **Row counts.** The sync pipeline's `num_output_rows` is 0 for every update, including the initial 5-row
  sync, so it cannot tell us how many rows changed. The M3 run log must take add/change/delete counts from
  our own MERGE metrics.
- **Throughput.** Measured once, on the CUDA torch build, on whatever architecture the run happened to get,
  with no repeat runs. Treat the figures as an order of magnitude. The CPU torch wheel (download.pytorch.org
  was reachable) is untested.
- **Runtime.** The run got serverless environment `client.2.5` with Python 3.11.10, not 3.12 (see D-005).

**Consequences.**
- **The smoke-test index, table and schema were deleted on 2026-10-15 at the owner's request** with
  `scripts/m0_cleanup.py` (`runs/m0_cleanup.json`). `scripts/m0_collect_evidence.py` can no longer be re-run,
  so `runs/m0_*` is the record.
- **The endpoint was deleted on 2026-10-19 at the owner's request** (`scripts/delete_endpoint.py`,
  `runs/m2_c_delete_idle_endpoint.json`: ONLINE with 0 indexes before, no endpoint listed 0.2 s after the
  delete). It was kept at first because Free Edition allows only one, but idle it billed 96 DBU a day, which
  likely counts against the undisclosed quota (see Quota), and nothing needs it before M5. M5 creates it again (STANDARD, same name)
  right before the real index; budget about 20 min, since the M0 index waited at least 1043 s in
  `PROVISIONING_ENDPOINT`. **The billing stopped** (`runs/m4_m_billing_after_endpoint_delete.json`, queried
  2026-10-20 07:46Z): `VECTOR_SEARCH` billed 4 DBU in every hour up to the deletion, 2.2382 DBU in the hour of
  it, and nothing after. The usage table's latest record then ended at 02:00Z, so it lagged about 6 h and the
  check covers about 20 h after the deletion.
- Unity Catalog keeps dropped tables recoverable with `UNDROP` and counts them when checking whether a schema
  is empty. A plain `DROP SCHEMA` therefore failed with "not empty" while `SHOW TABLES` returned nothing, and
  the cleanup had to force-drop after checking `SHOW TABLES DROPPED` held only the two smoke-test objects
  (`runs/m0_cleanup.json`). Tearing down a dev schema (M7) needs the same check.
- Create the production index once, after `gold_chunk_embeddings` holds the full first backfill (v3.5.0),
  so its capacity is sized for the corpus. If capacity errors still appear, recreate the index rather than
  trying to grow it.
- Call `index.sync()` in the same job right after each gold MERGE, wait for that update to complete, and log
  its pipeline update id. (*D-014 moved the sync into its own job, to run right after the committed row: by hand
  until M7's job graph chains it.*) This keeps the CDF gap short. CDF records "are transient and only accessible for a
  specified retention window"
  ([change data feed](https://docs.databricks.com/aws/en/tables/features/change-data-feed), updated
  2026-10-09), so an index that goes a long time without a sync may be unable to read the changes it missed.
  A likely recovery, untested, is a full-refresh sync, or dropping the index and recreating it from gold
  (about 19 min in M0).
- Sync start-up was 49.0 s for the sync update and 59.8 s for the initial one (n=2), so sync once per run, not
  once per batch.
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

**Reproduced 2026-10-15** (`runs/m1_h_bronze_vs_git_files.json`, `scripts/m1_verify_bronze.py`): git gives 19/96/51
for v3.5.0 → v4.0.0 and 1/4/0 for v4.0.0 → v4.0.1, and the same diff computed from bronze rows alone matches
both exactly. The table's 244/212/213 file counts match bronze's row counts per tag.

---

## D-003 — Where the processing runs

- **Date:** 2026-10-14 · **Status:** Accepted 2026-10-15 (implemented in M1)

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

**In M1.** `jobs/bronze_ingest.py` reads the archive with the pure `docs_rag` core on the driver; Spark does
only the Delta reads, the MERGE and the checks. A whole tag load (download a 34.6–48.6 MB archive in 4.0–4.9 s,
parse, MERGE) ran in 45.5–74.0 s per job, serverless start-up included (`runs/m1_a_*`, `m1_d_*`, `m1_e_*`).

---

## D-004 — Milestone order: DQ before gold, CI from M1

- **Date:** 2026-10-14 · **Status:** Accepted 2026-10-15 (CI done in M1; the DQ gate lands in M4)

The brief lists DQ checks after embeddings and CI with deployment. Two changes:
- **DQ runs as a gate between silver and gold (write-audit-publish).** If a check fails after we have
  already written to the index source table, the index ends up half updated. Checks must run on the
  staged change set before any publish.
- **CI runs the unit tests from M1.** It costs nothing, and every later milestone gets a green or red
  signal. Done: `.github/workflows/ci.yml` runs `uv sync --locked`, ruff, `mypy --strict` and pytest on every
  push and pull request (actions/checkout v7.0.1, astral-sh/setup-uv v10.3.0, read-only token). First green
  run: [38726258303](https://github.com/m3rcury02/spark-rag/actions/runs/38726258303).

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
  ("Python: 3.11.10", updated 2026-09-11). Our `pyproject.toml` requires `>=3.12,<3.13`, so the package would
  not even install there.
- **Consequence: pin the environment from the first Databricks job (M1), not M7.** "Notebook tasks default to
  Notebook Environment, which uses the notebook's own configured base environment and dependencies", and a
  job-level environment overrides it
  ([serverless dependencies](https://docs.databricks.com/aws/en/compute/serverless/dependencies), updated
  2026-09-29). So every job (runs/submit payload and bundle YAML) declares an `environments` entry with
  `environment_version` 3 or later, and each task references it with `environment_key`.
- **Pinned 2026-10-15: environment version 4 for every job** (`scripts/run_job.py`). Versions 3–6 all ship
  Python 3.12.3, so the choice between them is low-stakes. 4 has been out since 2025-08-13 and is supported
  until 2028-08-13, well past this project; 5 and 6 add Databricks Connect 18/19, which we do not need
  ([release notes](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/), updated
  2026-10-12). **Verified by a run:** Python 3.12.3, `DATABRICKS_RUNTIME_VERSION` `client.4.10`, Databricks
  Connect 17.3.14 (`runs/m1_a_v3.5.0_first.json` → `environment`). The job fails unless Python is 3.12, the
  runtime is `client.4.*` and Databricks Connect is 17.x; exact patch versions are recorded in each run, not
  enforced. So it does not rely on the Python check alone, which envs 3–6 would all pass.
- **The package version comes from git** (hatch-vcs), e.g. `0.0.1.dev11+g2474cafb4`. "If you change the
  implementation of a custom Python package used in a job on serverless, you must also update its version
  number so that jobs can pick up the latest implementation" (serverless dependencies page). The job asserts
  it imported exactly the wheel built for the run; run (c4) imported `0.0.1.dev13+g77dd68f4f` after a code
  change (`runs/m1_c4_*`).
- **A job script in a Volume is addressed as `dbfs:/Volumes/...`.** A bare `/Volumes/...` path is looked up
  in the workspace file tree and failed with "Cannot read the python file" (`runs/m1_a0_*`); the
  [Python script task docs](https://docs.databricks.com/aws/en/jobs/python-script) (updated 2026-09-11) give
  the `dbfs:` URI form for scripts in a volume.

---

## D-006 — Bronze: one immutable snapshot per tag

- **Date:** 2026-10-15 · **Status:** Accepted (evidence: `runs/m1_*`)

**Context.** Bronze must hold raw docs with source path, version tag, content hash and ingest time, and a
re-run must never create duplicates (brief item 1). M3 builds change detection on top of it (D-002 sequence).

**Options.**
1. One row per `(source_repo, version_tag, path)`: a snapshot per tag, kept side by side.
2. One row per `path` holding the latest version (SCD1), with history from Delta time travel or CDF.
3. SCD2 rows with valid-from/valid-to per path.

**Decision.** Option 1, and each tag's snapshot is **immutable**.
- Loading a tag that bronze already holds must reproduce it exactly: same paths, same content hashes, same
  commit, and every stored row intact (its content still hashes to its stored hash) and stored once. Any
  difference stops the run before anything is written (`docs_rag.bronze.check_against_stored`), because it
  means the fetch is broken, the tag moved or bronze was altered.
- Re-ingesting a changed tag (delete that tag's rows, then load it) is a deliberate manual step, and only for
  a tag that was never published. Bronze is keyed without the commit, so it holds one snapshot per tag; a
  published tag that moved is an incident that needs a new decision, not a re-ingest.
- New tags go in with an insert-only MERGE on `(source_repo, version_tag, path)`. A tag that is already stored
  identically skips the MERGE.

**Why.**
- "Re-running changes nothing" has an obvious definition (same tag in, nothing written) and an obvious check.
- M3's change detection becomes a diff of two snapshots by path and hash, and deletes show up as paths missing
  from the newer tag. That diff already reproduces D-002 from bronze alone (`runs/m1_h_bronze_vs_git_files.json`).
- A design review (2026-10-15) rejected the first draft, which had `WHEN MATCHED ... UPDATE` and a tag-scoped
  `WHEN NOT MATCHED BY SOURCE ... DELETE`. Release tags do not move, so those branches had no legitimate trigger,
  and a silently partial fetch would have turned the delete into data loss. Now a partial fetch fails (D-007)
  and a mismatch fails (this check), so neither branch is needed.
- Storage is trivial: 2.7–2.8 MB of docs per tag.

**Evidence.** Runs a, b, c2, c4, d, e, g2 and g4 are one-off serverless runs of `jobs/bronze_ingest.py` via
`scripts/run_job.py`. c1, c3, g1 and g3 are SQL statements run on the starter warehouse by `scripts/run_sql.py`.
f and h are `scripts/m1_verify_bronze.py` (git plus a warehouse query).

| Run | What happened | File |
|---|---|---|
| a | v3.5.0 first load: check `new`; MERGE inserted 244 rows in one commit (version 1), 0 rows copied | `m1_a_v3.5.0_first.json` |
| b | v3.5.0 again: check `identical`; MERGE inserted 0 rows and added or removed 0 files, **but Databricks still wrote an empty table version (2)** | `m1_b_v3.5.0_rerun.json` |
| c1 | Tamper: overwrote the stored hash of `docs/index.md` (version 3) | `m1_c1_tamper.json` |
| c2 | v3.5.0 again: **failed** with "content changed: docs/index.md"; nothing written (latest version still 3) | `m1_c2_*.json`, `m1_c3_restore.json` |
| c3 | Restored the hash from `sha2(content, 256)`; 244 docs, 0 mismatches. The restore is version 4 (c4's `version_before_merge`) | `m1_c3_restore.json`, `m1_c4_*.json` |
| c4 | v3.5.0 again, with the skip in place: check `identical`, MERGE skipped, **table version 4 → 4** | `m1_c4_*.json` |
| d, e | v4.0.0 (212 docs, 9 in nested folders) and v4.0.1 (213) inserted in one commit each; earlier tags untouched | `m1_d_*.json`, `m1_e_*.json` |
| f | First comparison with git, totals only: per-tag docs, bytes and commit ids, and the bronze-only diffs equal D-002 (15 checks), plus 9 bronze self-checks that the file also labels "git". Superseded by h | `m1_f_bronze_vs_git.json` |
| g1 | Tamper the case the hash column cannot show: appended a space to the **content** of `docs/index.md` (hash untouched) and inserted a duplicate `docs/quick-start.md` row (versions 7, 8). The c1–c4 code would have called this `identical` | `m1_g1_*.json` |
| g2 | v3.5.0 again: **failed** with "stored content does not match its stored hash: docs/index.md; stored more than once: docs/quick-start.md"; nothing written (latest version still 8) | `m1_g2_*.json`, `m1_g3_restore.json` |
| g3 | Restored both; 244 rows, 244 paths, 0 hash and 0 size mismatches. The UPDATE and DELETE leave the table at version 10 (g4's `version_before_merge`; 9 is inferred as the UPDATE's commit) | `m1_g3_restore.json`, `m1_g4_*.json` |
| g4 | v3.5.0 again: `identical`, MERGE skipped, version 10 → 10, invariants checked and none violated | `m1_g4_*.json` |
| h | **Every file compared with git**: for 669 files over three tags, the git blob id computed from bronze content equals git's, with no missing or extra paths; commit ids and D-002 diffs match (24/24 vs git, 9/9 invariants) | `m1_h_bronze_vs_git_files.json` |

Run (b) is why the job skips the MERGE on an identical snapshot: open-source Delta skips empty commits by
default, but this engine committed one, and a re-run should leave the table history alone too.

**Consequences.**
- **M3's diff baseline is the last *published* `(version_tag, commit_sha)`**, recorded in `ops_run_log` only
  after the gold MERGE and the index sync have completed. Never "the latest tag in bronze" and never ordering by
  `ingested_at`, which is audit-only: bronze can be ahead of what is published when the M4 gate stops a run.
- Bronze snapshots of a tag that was ever published are never deleted.
- Columns: `source_repo, version_tag, commit_sha, path, content, content_sha256, size_bytes, ingested_at,
  ingest_run_id`, all NOT NULL. `content_sha256` is SHA-256 of the raw file bytes. Content is decoded as strict
  UTF-8 with no normalisation, so `sha2(content, 256) = content_sha256` must hold. Every run checks it for every
  tag, together with one row per path, one commit per tag and `octet_length(content) = size_bytes`, and **fails**
  if any is violated (run g2 shows the snapshot check catching it first).
- Immutability is enforced by detection, not by the storage layer: a plain `UPDATE` works (runs c1, g1).
  `delta.appendOnly` would block it; not adopted in M1 because detect-and-fail covers a single-writer pipeline.
  It is a cheap hardening to consider for prod in M7.
- Two concurrent first loads of the same tag can both pass the snapshot check, because check-then-MERGE is not
  atomic. The later run's post-MERGE invariant check would then fail on the duplicate paths (not tested). M7's
  jobs should set one concurrent run per job.

---

## D-007 — Fetch: one source archive per tag, verified complete

- **Date:** 2026-10-15 · **Status:** Accepted (evidence: `runs/m1_*`)

**Options.** (a) GitHub's codeload tarball of the tag: one request. (b) The git trees API to list files, then one
raw.githubusercontent.com request per file (~245). (c) Fetch outside Databricks (CI or locally) and upload to a
Volume.

**Decision.** (a), inside the Databricks job. M0 showed serverless reaches codeload.github.com.

**Why.** One request is one atomic snapshot of the tag, with the commit id in the archive's pax header (git
archive writes it there). (b) has ~245 failure points, and the trees call counts against GitHub's 60/hour
unauthenticated API limit. (c) adds a moving part for no gain while the job can reach GitHub. Measured: the
archives are 34.6 MB (v3.5.0) and 48.6 MB (v4.0.0, v4.0.1). Downloads took 3.9–4.9 s in 7 of 8 runs and 0.5 s
once (run c4, a repeat download of v3.5.0, probably served from a cache) (`runs/m1_*` → `download`). The
commit ids read from the archives equal git's (`runs/m1_h_bronze_vs_git_files.json`).

**Completeness.** A snapshot is all or nothing, because D-006 treats it as the whole tag. `read_snapshot`
decompresses the whole archive (gzip checks CRC and length), requires the tar end-of-archive marker, one
top-level folder, at least one doc and a commit id, and decodes strict UTF-8. Python's streaming tar reader
(`r|gz`), used in the first draft, stops silently at a truncation; the design review reproduced this, and unit
tests now cover both truncation kinds and a real `git archive`.

**Filter.** Regular files under `docs/` ending in `.md`, at any depth. 4.x tags have nested folders such as
`docs/streaming/`, so a top-level-only filter would drop 9 files in v4.0.0 (`runs/m1_d_*` → `nested_docs`).

---

## D-008 — Cleaning: Jekyll pages to plain Markdown text

- **Date:** 2026-10-19 · **Status:** Accepted (evidence: `runs/m2_e_stability.json`, the silver runs
  `runs/m2_d1`..`m2_d6`)

**Context.** The Spark docs are Jekyll pages: YAML front matter, Liquid tags, raw HTML (config tables, language
tabs, layout tables, hidden divs), kramdown extras and Markdown links. A chunk is both what the model embeds and
what an answer cites, so leftover markup costs twice. BERT's tokenizer makes every punctuation character a
token, so a 99-character table border costs 99 tokens.

**Decision.** `docs_rag.clean.clean_page`, a fixed sequence of steps (module docstring), versioned by
`CLEANER_VERSION`:
- **Liquid from an explicit table.** An unknown tag raises, so a new Spark tag fails the run instead of leaking.
  `include_example` becomes `[example: <file>(, <label>)]`; the example code is not loaded (bronze holds `docs/`
  only). `{{ site.X }}` stays literal: substituting the release version would change every page that mentions
  it, in every release, and so its chunk IDs.
- **Code is masked before any HTML or Markdown rule** (CommonMark fences, `<pre>`/`<xmp>`, inline spans),
  because code is full of `<...>` that is not HTML. Liquid, hidden `d-none` blocks and the MathJax preamble are
  handled on the raw page before that, as Jekyll does, so Liquid inside code is processed too. A **conservation check** raises if masked code is missing from the output, except code
  inside markup that is dropped on purpose (HTML comments, `<style>`/`<script>`, image alt text, reference
  definitions). Cleaning may move code, never lose it.
- **HTML.** Only real tag names are stripped (an allowlist, matched to a name boundary, so `<col-name>` in prose
  stays). `<style>`/`<script>` go with their content. Tables are rendered innermost first as GFM tables (header
  and `---` delimiter when the first row is all `<th>`): `rowspan` cells are repeated into the rows they cover,
  `colspan` keeps the column count unless the spanning cell ends its row (trailing empty cells are dropped, so
  such a row has fewer cells), a caption becomes a line above, list items in a cell are joined, a nested
  table is written inline in its cell, one-line code in a cell stays inline and longer code follows the table, a
  one-column table keeps its pipes. A table with no `<tr>` is a layout table and is unwrapped. Language tabs
  become a `<lang>:` label line (from CLEANER_VERSION 2, none when the tab opens with a code block fenced in
  that language; D-013).
- **Markdown.** Links keep their text (titles in all three forms, `<url>` targets, one level of parentheses,
  wrapped and unterminated URLs); images, reference definitions, kramdown `{:...}` lines, the TOC bullet and
  `***`/`___` rules go; footnotes become `Note: ...`; `<br>` inside a pipe-table row is a space.
- **Borders.** Runs of 4+ `-`/`=` on border-only lines are shortened to 3, inside code too. A MathJax macro
  preamble is dropped.
- **Residue gate.** The silver run fails if any count is non-zero: Liquid outside code; real HTML element names
  (counted before entities are unescaped, so `&lt;U&gt;` in prose is not a tag); link syntax; table delimiter
  rows without a one-line header row (the chunker could not repeat that header).

**Evidence** (`runs/m2_e_stability.json`, 254/32, v4.0.1, unless noted; the `ablations` key reruns the
corpus with one rule off):
- The residue gate passed on the real corpus: all six silver runs (3 tags × 2 configs, `runs/m2_d1`..`m2_d6`)
  report `chunking.violations: []`, and that check includes "no doc with markup left after cleaning".
- Border shortening removes 25,485 of 601,105 cleaned tokens (4.2%): tokens on border-only lines drop from
  41,250 to 15,765. Embedded tokens drop from 720,005 to 692,251 (3.9%), chunks from 5,609 to 5,462.
- An implementation review (2026-10-18) and a fix-verification pass (2026-10-19) found real-page constructs the
  first version mishandled. The worst was a layout table whose code was silently dropped; that led to the
  conservation check. Each fix has a unit test, often on a quoted page structure (`tests/test_clean.py`).
- The unit fixtures could not hold every construct: the last round of fixes changed the real corpus
  (`scripts/corpus_fingerprint.py` failed) but not the fixture goldens (`git show --stat 6e5accf`: of the
  goldens, only `corpus_c1_k1.json` changed). Hence the corpus golden (D-010).

**Kept as is, on purpose (M6 questions, not M2 work).** Language tabs repeat near-identical prose; example
pointers carry no code; stub pages ("This document has moved here.") are one small chunk each. Each is a
retrieval-quality question for M6's recall@5/MRR, and changing one later is a version bump, not a redesign
(once a chain exists, the bump crosses it through D-013's re-baseline path).
Known limitation: on one page (spark-connect-overview.md) a heading inside the Python tab also heads the start
of the Scala tab, because tabs become plain label lines.

## D-009 — Chunking: section-aware, token-bounded, verbatim slices

- **Date:** 2026-10-19 · **Status:** Accepted (evidence: `runs/m2_e_stability.json`,
  `runs/m2_f_tokenizer_parity.json`)

**Context.** The brief asks for stable chunk IDs, section-heading metadata and configurable size and overlap;
chunking *within* sections is our choice (the M2 milestone line in CLAUDE.md), tested below against a
whole-page baseline. M6 compares 2 chunk sizes × 2 models. all-MiniLM-L6-v2 truncates at 256 word-pieces and bge-small-en-v1.5 at 512
(`max_seq_length` in the parity run). Both ship the same vocab.txt (sha256 `07eced37…`, recorded per model in
the parity run), vendored in `src/docs_rag/data/`.

**Options.** (1) fixed-size windows over the page; (2) greedy packing over the whole page (the "whole-page
baseline" below: same cleaner and packer, the page is one section, the breadcrumb is the title); (3) greedy
packing within heading sections (chosen); (4) content-defined boundaries.

**Decision.**
- Sizes are in **the models' own units**: WordPiece counts from the vendored vocab (`tokenizers==0.23.3`, pinned
  and checked by the job). `max_tokens` bounds the whole embed text `<breadcrumb>\n\n<body>`, with breadcrumb
  `title > h1 > h2 ...`; every `n_tokens` is a recount. Grid for M6: **254/32 and 128/16**: both fit MiniLM's
  256 positions with [CLS] and [SEP], with overlap at about an eighth of the budget (32 of 254, 16 of 128). 254
  is beyond the 128 tokens MiniLM was trained on ("The sequence length was limited to 128 tokens", its [model
  card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2), read 2026-10-19); whether that hurts is
  what the 128 config tests.
- Units are paragraphs, GFM tables and fenced code blocks. A unit that fits stays whole. One that does not is
  split: prose at sentence ends first (its line breaks are the source's hard wrapping; "e.g."/"i.e." do not end
  a sentence), a list at its item starts, code at its lines; then lines, sentences and word starts. Pieces
  leave room for the overlap tail.
- **Every chunk body is a verbatim slice of the cleaned page** (`char_start/char_end`), except inside a frame:
  a split code block gives slices of its inner lines inside the block's fences; a table too big for one chunk
  gives **one chunk per row under its header row** (a long row's later pieces also carry its first cell). Rows
  never share a chunk. Each chunk carries its frame, and `chunk_document` raises unless
  `body == frame_head + clean_text[char_start:char_end] + frame_tail` for every chunk, so every silver run
  enforces it (the frames themselves are not stored in silver).
- Overlap is best effort: the previous chunk's last ≤ overlap tokens, starting after whitespace (at a line start
  in code), never out of a whole code block and never from one frame into anything else.
- No merging of tiny sections; headings with no body give no chunk.
- **Tried and dropped:** moving a dangling lead-in line ("python:", "For example:") to the next chunk. In a dry
  run the overlap already repeated most of them at the start of the next chunk (output not saved, so no figure
  here); the rule was removed.

**Evidence.**
- Corpus (`tags` key; 254/32 then 128/16): 5,589 / 5,449 / 5,462 and 8,701 / 8,822 / 8,845 chunks for v3.5.0 /
  v4.0.0 / v4.0.1. Every chunk rebuilds exactly from its offsets and frame (`chunks_by_frame` has no mismatch).
  One doc has no chunks: `docs/sql-ref-functions-builtin.md`, which is only headings over generated
  includes (`{% include_api_gen ... %}`, not in bronze; `Config.zero_chunk_docs`).
- Token parity (`runs/m2_f_tokenizer_parity.json`): every distinct embed text of both configs and all three tags
  (13,655) through each model's own `preprocess`: 0 mismatches against `n_tokens + 2`, for both models. The
  longest input is 256 positions; 72 texts are exactly at MiniLM's limit and none is over it, so no chunk is
  truncated.
- Overhead: at 254/32 the model embeds 692,251 tokens for 575,620 cleaned tokens (v4.0.1), 20% more, from
  breadcrumbs, overlap and frames; at 128/16, 780,970 (36% more).
- One-chunk-per-row tables (`table_rows_off` ablation, 254/32, v4.0.1): 5,462 chunks instead of 3,908 and
  692,251 embedded tokens instead of 661,190 (+4.7%), median chunk 108 tokens instead of 192. A cost paid for
  self-contained rows; M6 tells whether it buys retrieval quality.
- Overlap (`behaviour`, 254/32, v4.0.1): of 3,721 places where one chunk follows another in the same section,
  1,027 prose joins overlap, 18 meet at a paragraph break and 1 inside a paragraph without overlap; 127 are at
  a whole code block (never overlapped); 2,548 involve a split code block or table row, of which 278 overlap
  (inside one split block or one long row) and 2,270 do not (a new row, or across frames, by design).

**What section-awareness does and does not buy (honest).** v3.5.0 → v4.0.0 (`transitions` key; 96 modified
docs):
| | section-aware | whole-page baseline |
|---|---|---|
| 254/32: share of a's tokens kept | 74.25% | 72.95% |
| 254/32: new tokens to embed | 215,764 | 210,243 (2.6% fewer) |
| 128/16: share of a's tokens kept | 76.06% | 77.34% |
| 128/16: new tokens to embed | 231,382 | 194,390 (16% fewer) |
| chunks wholly inside unchanged sections, kept (254 / 128) | 1,725/1,725 · 3,124/3,124 | 756/764 · 1,881/1,893 |

- Section-awareness **does not reduce re-embedding cost**; at 128/16 it costs 19% more, because a doc gives more,
  smaller chunks and each carries a longer breadcrumb. What it buys is locality: an edit to a section's text
  never changed a chunk of a section whose heading path and text are both unchanged (all kept), while the
  baseline lost 8 and 12 such chunks. A heading or title rename does change every chunk beneath it, because the
  breadcrumb is part of the ID: 27 lost IDs at 254/32 (45 at 128/16) for v3.5.0 → v4.0.0, and 6 of the 8 lost
  at 254/32 for v4.0.0 → v4.0.1 (the `breadcrumb rename` buckets).
- The largest cause of lost IDs whose own text did not change is table headers: 329 table rows at both sizes
  lost their ID only because their header row changed (`table row: own text unchanged, frame edited`), so
  every row of such a table re-embeds. The baseline shares this cost (same packer, same row rule). Split code
  pieces that lost their ID with unchanged text (9 and 11) are boundary shifts, not fence edits.
- v4.0.0 → v4.0.1 (a patch release, 4 modified docs) is almost the same for both: 21 vs 19 new IDs at 254/32.
- Unchanged docs keep identical chunk IDs in both transitions (97/97 and 208/208).

So the case for sections is retrieval quality (coherent chunks with a precise breadcrumb), which M6 measures,
not ID stability or embedding cost. If M6 shows no gain, the whole-page baseline is the simpler choice.

## D-010 — Chunk identity and the silver contract

- **Date:** 2026-10-19 · **Status:** Accepted (evidence: `runs/m2_d1`..`m2_d8d`, `runs/m2_g_silver_vs_local.json`;
  the runs waited out a compute refusal, `runs/m2_d0*`, D-001 Quota)

**Decision.**
- `chunk_id = sha256(json[path, section_path, embed_text, occurrence])[:32]`: content-addressed, with nothing
  tag-, position- or config-specific. An unchanged section keeps its IDs in every tag; any change to the
  embedded text, breadcrumb included, gives a new ID. `config_id = sha256(cleaner, chunker, max, overlap,
  vocab)[:16]` keys silver rows only.
- `silver_chunks` is keyed `(config_id, source_repo, version_tag, chunk_id)`, with one immutable chunking per
  `(config_id, tag)`, the bronze pattern: identical → MERGE skipped; different → the run fails, and names
  bronze moving under the stored rows before a missing version bump. Every run rechecks every stored row of its
  config (chunk_id recomputed from path, section path, embed text and occurrence; embed text = breadcrumb +
  body; n_tokens recounted; versions and sizes) and **re-chunks every other stored tag of the config from
  bronze, which must reproduce exactly**, before writing. Rows of other configs are not rechecked. Off-grid configs are refused. A doc without chunks fails the
  run unless it is in `Config.zero_chunk_docs`.
- **Version discipline.** Golden files are never overwritten: fixture text, fixture chunks, and the real corpus
  (`tests/golden/corpus_c<cleaner>_k<chunker>.json`, checked in CI and by `run_job.py` before every silver job).
  Changed output needs a version bump, which gives a new config_id and new rows; a new grid config or tag only
  adds entries. Version 1's goldens were recreated during M2 before anything was published to silver.

**Evidence** (2026-10-19, `workspace.docs_rag_dev.silver_chunks`):
- First runs (`m2_d1`..`m2_d6`): each inserted exactly the local chunk count (5,589 / 5,449 / 5,462 at 254/32,
  8,701 / 8,822 / 8,845 at 128/16), with the MERGE's own metrics showing inserts only (`numTargetRowsInserted`
  = `numSourceRows`, 0 updated, 0 deleted). Every later run of a config re-chunked the earlier tags from
  bronze and found them identical (`other_tags_reproduced`).
- Re-run (`m2_d7`, v4.0.1 at 254/32): identical, MERGE skipped, table version unchanged (7 → 7).
- Tamper (`m2_d8a`..`m2_d8d`): one character appended to one stored v3.5.0 `embed_text`
  (`docs/index.md` chunk 0). The next run, **of v4.0.1**, failed before writing: "stored silver rows of
  v3.5.0 fail their recheck: … chunk_id does not match its content; … embed_text is not breadcrumb + body;
  … n_tokens 227 is wrong or over 254". After the restore the same run was identical and wrote nothing (version 9 → 9).
- Silver vs a local chunking of git (`m2_g`): all six (tag, config) agree row for row on path, chunk_id,
  chunk_index, n_tokens, occurrence, offsets, section_path, the sha256 of body and embed text, the doc hashes,
  commit and versions; every local body rebuilds from its offsets; and the fingerprint recorded by each of the
  nine silver jobs (`job_runs_compared`, the failed tamper run included), computed on serverless aarch64,
  equals this machine's (x86_64). So the chunk IDs did not depend on these two machines.
- The runs span four wheel builds (0.0.1.dev46, dev47, dev49, dev51) because the version comes from git and
  runs/ commits landed between runs. The code was the same: `git diff 2788b57 e841d54 -- src jobs
  pyproject.toml uv.lock` is empty.
- **Table versions are not only ours.** Version 6 is an `OPTIMIZE` with `auto: true` (34 files into 1,
  exactly the files MERGEs 1-5 added) at 08:11:02Z, 5 s after run 5's MERGE and while run 5 was still running;
  `auto: true` marks auto compaction, which our MERGE most likely triggered (inference). Version 10 another `OPTIMIZE` (`auto: false`) at
  08:27:24Z after our last run (`runs/m2_h_silver_table_history.json`). We never run OPTIMIZE; that the second
  one is predictive optimization (a `PREDICTIVE_OPTIMIZATION` line also shows in billing on 10-15) is
  inference. Our runs read the version right before their MERGE (`runs/m2_d6` starts from
  `version_before_merge: 6`), so neither did harm; M3 must never infer "we wrote" from a version bump.

**Why the corpus golden.** A cleaner or chunker change that passes every unit test can still change silver
rows (the last cleaner round did, D-008). The job's re-chunk of the other stored tags catches most such
changes, but not for the first tag stored under a config or a change that touches only the new tag's pages.
The corpus golden closes those cases, and it fails in CI, before any Databricks run.

**Consequences.**
- M3: the diff baseline is the last *published* (tag, commit) per config (D-006). Chunk-level change = added
  and removed chunk_ids; there is no chunk-level "changed".
- M5: gold and the index carry only chunk_id-determined columns (path, section_path, embed_text, n_tokens,
  vector, model); positions (`chunk_index`, offsets) are read from silver of the published tag, because kept
  chunks move (D-011: 2,038 of 3,563 for 3.5 → 4.0). The gold MERGE is insert/delete only and asserts
  numUpdatedRows = 0; what to embed and delete is the chunk diff (D-011).
- M6: label answers as (path, section_path, verbatim quote), resolve the quote to every occurrence in the
  cleaned text (the offsets name its coordinate system, `CLEANER_VERSION`), and count a chunk relevant if its
  slice overlaps any occurrence.

## D-011 — Change detection and the run log

- **Date:** 2026-10-20 · **Status:** Accepted (evidence: `runs/m3_*`)

**Context.** Brief item 3: "detect added, changed and deleted documents by hash. Re-embed only changed chunks
and remove chunks for deleted docs. Log the counts for every run." The milestone adds "v1 → v2 → re-run", and
D-002 fixes the sequence v3.5.0 → v4.0.0 → v4.0.0 again → v4.0.1, with git's counts as the oracle. D-006 says
the diff baseline is the last *published* snapshot, never "the latest tag in bronze". D-010 makes chunk
change a set difference of chunk_ids. A design review by three independent critics (2026-10-19) rejected the
first draft on the points marked "review" below.

**Options.**
- *Where the baseline comes from.* (1) The latest bronze tag or `ingested_at`: rejected by D-006, since bronze
  can be ahead of what was published. (2) A pointer table updated by every run: a second write that can
  disagree with the log after a crash. (3) The latest committed run-log row by `finished_at` (first draft):
  rejected in review, because timestamps can tie, an edited row goes unnoticed, and an emptied log looks like a
  first run. (4) **Chained rows** (chosen): each row names the run it diffed against, and the baseline is the
  head of that chain.
- *How changes are captured.* Delta Change Data Feed on bronze would only show the inserts of a whole new tag,
  since bronze holds insert-only snapshots per tag (D-006); it cannot say which path changed. A snapshot diff
  by path and content hash (chosen) can, and it reproduces D-002 from bronze alone (`runs/m1_h`).
- *Trust silver or check it.* The first draft read silver's chunk IDs as they were; a tampered `embed_text`
  with its ID untouched (M2's tamper, `runs/m2_d8a`) would have passed (review). Chosen: re-chunk both sides
  from bronze and require silver to match row for row, the M2 recheck.
- *Detail.* Doc paths as arrays in the run-log row (at most a few hundred strings, written in the same
  insert) rather than a separate per-doc table; chunk changes as counts, since silver holds the IDs.

**Decision.** `jobs/change_detect.py` (logic in `docs_rag.changes`, Spark I/O in `docs_rag.lake`):
- It runs for the published chunk config (`Config.chunk`, 254/32); M6's 128/16 config is never published.
- **Baseline:** the head of the chain of `committed` rows for `(source_repo, sink)` in `ops_run_log`. Zero or
  several heads, a dangling link or a second initial run fail the run (`pick_baseline`). With no chain the run
  needs `--initial`; a target older than the baseline (release-number order), or one that cannot be ordered,
  needs `--allow-backwards`. Every row records its `direction`.
- **Both sides are verified first.** The target tag's bronze is whole and silver holds exactly its chunking
  (re-chunked and compared). The baseline's bronze still holds the commit and docs fingerprint its run logged,
  and silver still holds its chunking under the baseline's config, with the logged chunk fingerprint.
- **Diff:** docs added / changed / deleted / unchanged by content hash (a rename is a delete plus an add, and
  is counted as `docs_moved`); chunks added / removed / kept by chunk_id, split by the kind of doc they belong
  to, which maps the brief's sentence onto columns (`chunks_added_changed_docs` is "re-embed only changed
  chunks", `chunks_removed_deleted_docs` is "remove chunks for deleted docs"). The chunk diff must fit the doc
  diff: under the same config an unchanged doc has exactly the same chunk IDs; a doc may lack chunks only if
  it is in `Config.zero_chunk_docs`. `tokens_to_embed` and the target's total tokens are logged.
- **Status.** A row is `committed` or `failed`. Only `committed` makes a baseline, and the job writes it as
  its last write, after every check; M4's DQ gate and M5's gold write and index sync go before it in the same
  job. A failure after the Spark session started appends a `failed` row (no counts, never a baseline). A
  failure before that (environment check, a refused submit) is recorded only in `runs/`.
- **Sink.** The chain is per sink, the thing a committed run published to: `none` in M3 and M4, the gold
  table from M5. So M5's first run finds no chain and is an explicit `--initial` load into an empty gold, and
  M3's rows never become a baseline for gold.
- `ops_run_log` is created with `delta.appendOnly = true`: rows are only ever inserted, so unlike bronze this
  costs nothing. Its columns are checked against `RUN_LOG_COLUMNS` on every run.

**Evidence** (2026-10-19, `runs/m3_*`; each detect run is a serverless job via `scripts/run_job.py`):

| Run | What happened | File |
|---|---|---|
| a | v3.5.0 without `--initial`: refused, "no committed run for sink 'none'"; logged `failed` | `m3_a_*` |
| b | v3.5.0 `--initial`: 244 docs added, 5,589 chunks, 667,028 tokens; `committed` (chain start) | `m3_b_*` |
| c | v3.5.0 → v4.0.0: docs 19 added / 96 changed / 51 deleted / 97 unchanged (= D-002); chunks 607 added in new docs + 1,279 in changed docs, 1,500 removed from changed docs + 526 from deleted docs, 3,563 kept; 215,764 of 689,988 tokens to embed (31%) | `m3_c_*` |
| d | v4.0.0 again: 0 / 0 / 0, all 5,449 chunks kept, 0 tokens to embed; a `committed` row of zeros | `m3_d_*` |
| e | v4.1.0 (not in bronze): failed, "no bronze rows for the tag"; logged `failed` | `m3_e_*` |
| f | v4.0.0 → v4.0.1: the baseline skipped the failed run (e); docs 1 / 4 / 0 (= D-002); chunks 13 + 8 added, 8 removed, 5,441 kept; 3,538 of 692,251 tokens to embed (0.51%) | `m3_f_*` |
| g | v4.0.0 after v4.0.1, no flag: refused as backward; logged `failed` | `m3_g_*` |
| h | silver v4.0.1 again after moving its Spark I/O into `docs_rag.lake`: identical, nothing written | `m3_h_*` |
| i | g again after turning retries off: one task attempt, one `failed` row | `m3_i_*` |
| j | v4.0.1 again on the final code: 0 / 0 / 0, `committed` | `m3_j_*` |

- **Independent check** (`m3_v2_changes_vs_git.json`, 102 checks, all pass): for every committed row, the path
  lists equal a plain comparison of git's files at both tags, the doc counts equal D-002, the chunk counts per
  doc kind equal set arithmetic over a local chunking, and for 3.5 → 4.0 and 4.0 → 4.0.1 the chunk counts and
  tokens equal `runs/m2_e` (computed by separate code); the five committed rows form one chain from one
  initial run; failed rows carry no counts; every job summary has its row. The verifier does not import
  `docs_rag.changes`. Its first run (`m3_v_*`) failed 50 of 83 checks on a verifier bug, the statement API returning
  numbers as strings; the data were right.
- **Retries found in the log.** Serverless retried every task whose script ran and failed, once, with the same
  parameters (task attempts 0 and 1, `m3_k_task_attempts.json`; M1's run a0, which failed before its script
  could be read, ran once), so runs a, e and g each left two `failed` rows, 39–56 s
  apart; the runs took 120, 83 and 77 s against 40 s for run i with one attempt. Harmless here (a failed row
  is never a baseline), but a retry after a commit would have committed the same run twice. Now
  `run_job.py` submits with `max_retries=0` and serverless auto-optimization (its automatic retry) off, and
  detect writes nothing if its run_id is already committed. M1's tamper runs (c2, g2) and M2's (d8b) were
  retried too (`m3_k`), without effect, since they write nothing on failure.
- **Read-only on the data:** bronze and silver have no history entry from the start of the sequence on
  (`m3_w_*`, `m3_w2_*`); `ops_run_log` has one WRITE per row (12). An `UPDATE` of the log is refused with
  `DELTA_CANNOT_MODIFY_APPEND_ONLY` (`m3_w3_*`).
- **Cost.** A detect run took 52–65 s of job time, including re-chunking two tags (`run_duration_seconds`).
  The re-embedding it spares is the point: a patch release changes 0.5% of the tokens to embed.
- Kept chunks move: 2,038 of the 3,563 chunks kept from 3.5 to 4.0 have a new `chunk_index` or offsets
  (`chunks_kept_relocated`), and 0 from 4.0 to 4.0.1. 17 of the 96 changed docs (3 of 4 in 4.0.1) changed
  no chunk at all (`docs_changed_same_chunks`; m2_e's 18 also counts the zero-chunk doc). No rename was
  detected as a move (`docs_moved` 0).

**Consequences.**
- M4: the gate runs inside this job, before the commit; a failed gate leaves the baseline where it was. The
  row has before/after totals for the drift check.
- M5: gold carries only chunk_id-determined columns. Positions (`chunk_index`, offsets) come from silver of
  the published tag, because kept chunks move (`chunks_kept_relocated`: 2,038 of 3,563 for 3.5 → 4.0). What to
  embed and delete is the chunk diff against the published state; M5 asserts that gold equals the baseline's
  chunk set (by fingerprint) before applying it, and that the MERGE counts equal the diff.
- M7: one concurrent run per job. Two concurrent runs can both read the same head; the second insert forks
  the chain, which the read-back and every later run detect (`ChainError`), but nothing prevents it before
  M7. Retries need a per-attempt key if task retries are turned on.

## D-012 — The DQ gate between silver and gold

- **Date:** 2026-10-20 · **Status:** Accepted (evidence: `runs/m4_*`, `runs/m4x_*`). Two versions of the gate:
  **v1** (M4) and **v2**, built the same day on the owner's decisions: drift thresholds by release kind (patch 5%,
  minor and major 10%; option (4) below) and an override bound to a failed run's logged drift (Decision). The
  owner's third decision, a re-baseline path for cleaner and chunker version bumps, is D-013.

**Context.** Brief item 5: "Data-quality checks: empty or tiny chunks, oversized chunks, duplicate chunks,
missing metadata, and row-count drift between runs. A failed check stops the pipeline." CLAUDE.md: a failed
check stops the run before anything is published (write-audit-publish); results go to `ops_dq_results`. D-011
put the gate inside `jobs/change_detect.py`, before the committed row. The M4 open question (from M0): v3.5.0 →
v4.0.0 legitimately drops 13% of the docs, so a hard drift check fires on real releases unless it has a
threshold and a logged override.

**Options.**
- *Where the gate runs.* A separate job between silver and gold, or inside the change-detection job (chosen),
  after the diff and before the committed row. One process gives one order of writes and no hand-off between
  jobs. In the job, the committed row is appended only through `commit()`, which takes the gate's
  `GatePassed`; M5's gold write will take it too. That token is a guard against mistakes in our own code, not a
  control: Python cannot stop code that builds one on purpose, and the append-only run log accepts any INSERT
  (e.g. through `scripts/run_sql.py`). In both designs, a commit without a gate is caught only after the fact,
  by the verifier (`scripts/m4_verify_gate.py`).
- *What the checks cover.* Only the added chunks (what M5 embeds), or all of the target's chunks. Chosen: all,
  because gold holds the whole target and a check on the change set misses a defect already in the baseline.
  Its cost was not measured on its own: on the same transitions as M3, M4's job took 2–9 s longer (Cost).
- *Thresholds.* Relative to the baseline (a share that may not grow) or absolute shares. Chosen: absolute
  shares of the target's chunks, with room above what the real corpus has (tiny: 2% against at most 0.92% at
  254/32 and 1.17% at 128/16; duplicate embed texts: 1% against at most 0.43%); a relative rule lets a slow
  regression creep in one release at a time. The room is a choice, not derived: tiny chunks can more than
  double, and duplicate embed texts at 254/32 grow 4.7 times, before either check fails. Drift has the creep
  problem itself: it compares with the previous committed run, so repeated losses of 9% each pass, and every
  override makes the overridden run the new reference. Drift catches step changes, not creep.
- *Drift.* (1) Hard fail, no override: fires on every big release and gets switched off (the open question).
  (2) Warn only: nobody reads warnings, and the brief says stop. (3) **One 10% threshold plus a logged
  override** (built in M4). (4) Thresholds by release kind, read from the tags as `changes.direction` already
  does: patch 5% with minor and major 10% fires on the same 3 of 41 transitions as (3) and is tighter where the
  corpus is most stable (the largest patch move is 2.2%, so a 5–10% move on a patch release passes (3) but
  fails (4)); patch 5% with 20% for the rest fires on none of the 41, its 20% just above the largest observed
  feature-release move (16.4%). (3) and (4) are picked from the same calibration table, so neither is less
  "fitted". (5) Drop `drift_docs` or make it loss-only: it causes every historical firing (chunks fire only
  once, together with docs; tokens never), and the brief's "row-count drift" can be read as chunks, the rows
  gold publishes. Kept because it sees what chunk counts hide: 4.0.0 deleted 51 docs while chunks fell 2.5%,
  and losing 51 pages is worth a human look even when it is a release. The owner chose (4) with patch 5% and
  minor and major 10% (2026-10-20), built as gate v2; M4's runs (the first table below) used v1's 10%.
- *Statuses.* A "warn" status was dropped: a check passes or fails; `overridden` and `not_applicable` exist
  only for drift.
- *Language-tab labels.* 8 of the 10 tiny chunks logged as examples for v4.0.1 are lone tab labels (`java:`,
  `scala:`, `r:`) left by the cleaner; the other two are one-item lists. The examples are the first 10 by path,
  not a sample, so the other 36 of the 46 are unclassified. Fixing the labels means a cleaner version bump and a
  silver re-run, and in M4 a bump could not cross an existing chain (Consequences). Chosen for M4: leave them;
  they were under the threshold. Mostly fixed afterwards by CLEANER_VERSION 2, the first use of D-013's
  re-baseline path: tiny chunks of v4.0.1 at 254/32 fell from 46 to 20. A tab that opens with prose or a table
  keeps its label, so some lone labels remain (e.g. `sql-ref-datatypes.md` #82 `r:`, in `m4x_q`'s examples).

**Decision.** `docs_rag.dq` (pure Python; thresholds in `Config.dq`), called by `jobs/change_detect.py`:

| Check | Fails when | Threshold |
|---|---|---|
| `empty_chunks` | a body has no WordPiece token (blank, or only characters the tokenizer drops), or an embed text is blank | any |
| `tiny_chunks` | bodies under 5 tokens, as a share of all chunks | > 2% |
| `oversized_chunks` | the larger of a recount of the embed text and its stored `n_tokens` over `min(max_tokens, 256 − 2)` | any |
| `duplicate_chunk_ids` | rows beyond the first of a repeated chunk_id | any |
| `duplicate_embed_text` | copies beyond the first of a repeated embed text, as a share | > 1% |
| `missing_metadata` | a path outside `docs/*.md`, a blank section name (title first), another `version_tag`, a commit that is not 40 hex | any |
| `drift_docs`, `drift_chunks`, `drift_tokens` | the target's total against the baseline run's: `|to − from| > threshold × from`, the threshold by release kind (`changes.release_kind`: the largest of major, minor, patch that differs; the same tag and tags that are not releases count as patch) | v2: 5% patch, 10% minor or major (v1: 10%); a loss over 50% cannot be overridden |

- **Override (v2): plan, then apply.** A drift failure is first seen in a failed run. The operator then runs
  again with `--accept-drift "<reason>" --accept-drift-of <that run_id>`. The named run must have exactly one
  run-log row, `failed` (enforced from commit `cfbf52e`; before it the job accepted any number of failed rows,
  and the runs below each named a run with one), DQ rows from one attempt with the same config, tags and
  baseline as this run, and failed drift checks only. A drift check of this run is `overridden` only if it fails with exactly the
  (check, numerator, denominator) that run logged, so the operator accepts numbers they saw, not "any drift".
  Refused before the Spark session: one flag without the other, a reason under 20 characters or 4 words, or
  `--initial`. Refused by the gate: an acceptance with nothing over its threshold ("nothing to accept"), or a
  run whose drift is not the named run's. A loss over 50% and any non-drift check are never covered. The reason
  and the run id go on every overridden `ops_dq_results` row (`override_reason`, `override_of_run_id`) and on the
  committed run-log row (`dq_override`, `dq_override_of`). Still not bound: the reason test is lexical (any four
  words pass), and nothing records who overrode. Only losses are capped: a loss deletes from the index and is
  what a broken input looks like, while a gain only adds (and costs embedding, which M5 must size). The 50% is a
  policy value, not calibrated. *v1 (M4)* took a reason alone, which covered every drift check at any size short
  of the ceiling: run h's reason ("deleted 51 docs") would also have passed a run that deleted 100.
- **Logging.** One `ops_dq_results` row per check per run attempt (an `attempt_id` per job process): counts,
  observed value, threshold, the rule in words, up to 10 examples, status. All nine rows go in one insert and
  are read back before the gate decides. The table is append-only. A failed gate appends a `failed` run-log row
  whose error names the failing checks; the baseline does not move.
- **Model length** is checked when `Config` is built: every chunk config must satisfy `max_tokens + 2 ≤
  embedding_max_seq_length` (256, all-MiniLM-L6-v2), so the oversized check is a backstop.
- **Which checks can fire on real data.** Drift and the two share checks follow the content, so a future
  release can trip them. Oversized chunks, duplicate IDs and missing metadata are backstops: silver is
  re-chunked from bronze and compared row for row before the gate (D-011), the chunker asserts its own size
  bound, and IDs hash the path and text, so on today's code they fire only after a regression that also passes
  the goldens. An empty chunk can come from content: a section holding only characters the tokenizer drops
  (zero-width spaces) gives a body with no token, which a review of this code showed. Unit tests
  (`tests/test_dq.py`) show each check firing.
- **Not caught.** Every check is structural: counts, token lengths, duplicates, non-blank metadata, totals. A
  content defect that keeps the counts in range passes, e.g. a new Liquid or HTML construct the cleaner leaves
  in the text, sections replaced by boilerplate, a wrong but non-blank breadcrumb, or drift under 10%. The
  goldens pin only v3.5.0, v4.0.0 and v4.0.1, so for a new release these checks are the only content guard, and
  "no release failed a count check" says nothing about such defects. Only `Config.chunk` is gated; the 128/16
  rows M6 evaluates are not. A cheap next check would be the share of bodies with leftover markup (`{%`, `{{`,
  `<div`, an unclosed fence), calibrated like the others.

**Evidence.**
- *Calibration* (`runs/m4_a_calibration.json`, git only): all 42 final releases from v3.0.0 to v4.2.0, chunked
  at 254/32, each against the previous release in version order. No release failed a count check: tiny
  chunks 0.83–0.92% (threshold 2%), duplicate embed texts 0.05–0.21% (threshold 1%), no empty, oversized,
  duplicate-ID or metadata failures (this run predates counting a body with no token as empty; the gate runs
  below, on the final rule, found no empty chunk in v3.5.0, v4.0.0 or v4.0.1). At 128/16, along D-002's path:
  tiny 1.14–1.17%, duplicates 0.32–0.43%.
  Drift at 10% fired on 3 of 41 transitions: none of the 33 patch releases (largest move 2.2%, docs of
  v3.0.0 → v3.0.1), 2 of 7 minor (3.3 → 3.4: docs +14.4%, chunks +16.4%; 3.4 → 3.5: docs +13.5%) and the one
  major (3.5.9 → 4.0.0: docs −13.1%). Tokens never moved more than 8.5%. Firing by threshold: 5% → 6 of 41,
  15% → 1, 20% → 0. So the 10% rule asks for a stated reason on about three of every eight feature releases,
  and so far never on a patch release.
- Doc counts are a poor proxy for the index's size: along D-002's path, 3.5.0 → 4.0.0 drops 13.1% of the docs
  but 2.5% of the chunks, and tokens rise 3.4% (`d002_path`). The docs check fires although the index hardly
  changes size (its content does: 31% of the tokens are new, D-011).
- *The gate on Databricks* (2026-10-20, chain of sink `none`, head v4.0.1 at the start; each detect run a
  serverless job via `scripts/run_job.py`):

| Run | What happened | File |
|---|---|---|
| b | `ALTER TABLE ops_run_log ADD COLUMNS (dq_override …)`: 44 → 45 columns, still append-only; the 12 earlier rows read NULL | `m4_b_*` |
| c | v4.0.1 again: all nine checks pass (tiny 46 of 5,462, duplicate embed texts 3, drift 0); `committed`. The first run also created `ops_dq_results` | `m4_c_*` |
| d | the same with `--accept-drift "<reason>"`: nine `pass` rows logged, then refused by the gate, "nothing to accept"; `failed` | `m4_d_*` |
| d2 | `--accept-drift "ok"`: refused before the Spark session ("at least 20 characters and 4 words"); no DQ row, no run-log row | `m4_d2_*` |
| e | v3.5.0 with `--allow-backwards`: `drift_docs` 213 → 244 (+14.6%) fails (chunks +2.3%, tokens −3.6% pass); `failed`, the baseline stays v4.0.1 | `m4_e_*` |
| f | the same with `--accept-drift`: `drift_docs` overridden, `committed` with the reason in `dq_override`; docs 51 added / 97 changed / 20 deleted, 194,079 tokens to embed | `m4_f_*` |
| g | v4.0.0: `drift_docs` 244 → 212 (−13.1%) fails; `failed` | `m4_g_*` |
| h | the same with `--accept-drift`: overridden, `committed`; docs 19 / 96 / 51 (= D-002) and 215,764 tokens to embed, as M3's run c | `m4_h_*` |
| i | v4.0.1: all pass, `committed`; docs 1 / 4 / 0 and 3,538 tokens, as M3's run f. The chain is back at v4.0.1 | `m4_i_*` |

- *Independent checks.* `m4_v_gate_vs_git.json` (501 checks, all pass) recomputes every DQ row of the seven runs
  that reached the gate from a local chunking of git, with the rules written out again (it does not import
  `docs_rag.dq`; it shares the chunker, verified in M2, and cannot check `version_tag`, which git chunks do not
  carry): counts, observed values, thresholds, statuses, override reasons; each run's outcome and the
  checks its error names; and, from Delta's own history (time travel over each table's WRITE versions), that
  each run's nine DQ rows went in one insert before its run-log row. Run d2 has no DQ or log row. Every
  committed run since the gate has a complete passing set; the five M3 commits have none, and their code
  predates the gate commit. (During development the verifier was also run on simulated tables with a changed
  count, a missing `dq_override` and DQ rows written after the commit, and failed each; that run was not saved.)
  `m4_w_changes_vs_git.json` re-runs M3's verifier over all 19 log rows: 184 checks pass, and the nine
  committed rows form one chain (v3.5.0 → 4.0.0 → 4.0.0 → 4.0.1 → 4.0.1 → 4.0.1 → 3.5.0 → 4.0.0 → 4.0.1).
- *What the job wrote, from Delta rather than the job:* nothing in bronze (last history entry 2026-10-15) or
  silver (an `OPTIMIZE` of 2026-10-19), and seven WRITEs of nine rows each to `ops_dq_results`, each before its
  run's row in `ops_run_log` (`m4_k_*`, and the verifier above). An `UPDATE` of `ops_dq_results` is refused with
  `DELTA_CANNOT_MODIFY_APPEND_ONLY` (`m4_l_*`). Each run had one task attempt (`m4_j_*`). M4 publishes nothing
  (sink `none`), so this shows the gate's order of writes; write-audit-publish for a real publish is untested
  until M5.
- *Gate v2 on Databricks* (2026-10-20, same chain, head v4.0.1; `runs/m4x_a`..`m4x_j`):

| Run | What happened | File |
|---|---|---|
| a | `ADD COLUMNS` `dq_override_of` (run log, 46 columns) and `override_of_run_id` (DQ results, 20); both still append-only | `m4x_a_*` |
| b | v4.0.1 again: all pass, drift threshold 5% (same tag counts as patch); `committed` | `m4x_b_*` |
| d | v3.5.0 with `--allow-backwards` (a major step): `drift_docs` 213 → 244 (+14.6%) over 10%; `failed` | `m4x_d_*` |
| e | the same, accepting M4's failed run e (`64e598eb…`): refused before the gate, since that run drifted from another baseline run (`e3a95072…`, not `ba641b3e…`); `failed`, no DQ rows | `m4x_e_*` |
| f | the same, accepting run d (`7a8d08da…`): `drift_docs` overridden with exactly d's 244/213; `committed` with `dq_override_of` = d | `m4x_f_*` |
| g | v4.0.0: `drift_docs` 244 → 212 (−13.1%) over 10%; `failed` | `m4x_g_*` |
| h | the same, accepting run g (`3767945a…`): overridden; `committed` | `m4x_h_*` |
| i | v4.0.1 (a patch step): drift +0.5% / +0.2% / +0.3% within 5%; `committed` | `m4x_i_*` |

  `m4x_v_gate_vs_git.json` (934 checks, all pass) re-checks every gated run of M4 and gate v2, each under the
  rules of its own version, chosen by whether the run's commit descends from gate v2's first commit; for v2 it
  reads the accepted run's DQ rows itself and checks that the run is a single failed run of the same config,
  tags and baseline that failed on drift alone. `m4x_w_changes_vs_git.json` re-checks the 26 log rows (265
  checks, all pass): 13 committed runs in one chain. Each run had one task attempt (`m4x_j_*`). The per-kind
  thresholds fire on the same 3 of 41 historical transitions as v1 (`m4_a`'s table: no patch release over 5%;
  `m4x_x` under cleaner 2 gives the same 3).
- *Cost.* Detect runs took 56–83 s of job time (`run_duration_seconds`; c: 104 s, 50 s of it before the
  script started, and it also created `ops_dq_results`; d2: 36 s). On the same transitions as M3: 3.5.0 → 4.0.0
  61.2 → 68.8 s (`m3_c`, `m4_h`), 4.0.0 → 4.0.1 64.9 → 73.4 s (`m3_f`, `m4_i`), 4.0.1 again 54.4 → 56.3 s
  (`m3_j`, `m4_d`). The gate's own share was not timed, and the differences are within what serverless start-up
  varies by.

**Consequences.**
- M5, order of steps (*superseded by D-014: the committed row comes before the index sync, which became its
  own job; there is no automatic resume; and there is no staging table, the vectors go from memory into the
  MERGE source*): chunk checks → embed into a staging table → vector checks (dimension, no NaN, norm),
  logged to `ops_dq_results` like the others, which the nine-check `gate` cannot take as it stands → gold MERGE
  with the token → index sync and wait → committed row. Vector checks after the MERGE would be audit after
  write, since gold is the index's source. A failure between the MERGE and the committed row leaves gold at the
  failed run's target while the baseline stays: M5 needs a recovery rule (e.g. a retry that finds gold equal to
  the target's chunk set re-applies idempotently instead of failing D-011's "gold equals the baseline" check).
- In M4 a cleaner or chunker version bump could not cross an existing chain: the baseline's logged `config_id`
  left `Config.chunk_grid`, so the job stopped; re-chunking the baseline with new code would not match its
  silver anyway; and `--initial` is refused while a chain exists. Solved by D-013's re-baseline path.
- M5's first run is `--initial` on a new chain, so its drift is not applicable: the first gold load is compared
  with nothing. M5 decides whether to compare it with the head of the `none` chain.
- The thresholds are a policy calibrated on 42 releases of one corpus; another corpus needs its own
  calibration run (`scripts/m4_calibrate.py`).
- M7: the override is a CLI flag on one run, and a schedule has no one to type it. Each firing re-runs the job
  (about a minute of serverless), fails again and appends nine DQ rows and a failed row until someone runs the
  release by hand with a reason. M7 needs a failure notification (otherwise this is the warn-only option again),
  and should fail fast on a target whose last gated attempt failed on the same tag, commit and config.
- With the bound override (v2), a schedule still cannot accept drift by itself: the accepting run names a run
  that has already failed, so a person reviews the numbers first, which is the intent.

## D-013 — Crossing a cleaner or chunker version: the re-baseline path

- **Date:** 2026-10-20 · **Status:** Accepted (evidence: `runs/m4x_k`..`m4x_y`, `m4x_v3`, `m4x_w3`)

**Context.** D-012 found that a CLEANER_VERSION or CHUNKER_VERSION bump could not cross an existing chain:
`config_id` hashes both versions, so after a bump the baseline's logged config is no longer in
`Config.chunk_grid`, `change_detect` stopped ("not in Config.chunk_grid"), re-chunking the baseline with the
new code would not match its silver anyway, and `--initial` is refused while a chain exists. Once M5 starts the
gold chain, every cleaner fix would have been blocked. The owner chose to build a re-baseline path rather than
fix the known cleaner issue first and freeze the cleaner (2026-10-20), since the path is needed for any later
fix anyway.

**Options.**
- *Freeze the cleaner before M5.* Postpones the problem to the first fix after M5. Rejected.
- *Start a new chain after every bump (`--initial` into an empty sink).* Re-embeds everything and breaks the
  audit chain; for gold it means dropping and rebuilding the index. Rejected.
- *Run the old code inside the job* (ship earlier versions in the wheel, or check out the baseline's commit):
  two versions of the cleaner in one job, and a job that depends on git history. Rejected for the job; used for
  verification (below), where it is cheap.
- *Check the baseline from what was stored* (chosen): silver is immutable per (config_id, tag) and each row's
  chunk_id is a hash of its own content, so stored rows can be checked without the code that made them.

**Decision.** In `jobs/change_detect.py`, step 4:
- A baseline whose config this code can reproduce is re-chunked from bronze and compared with silver, as
  before (D-011). One it cannot is checked from its stored silver rows (`lake.stored_baseline`,
  `silver.stored_baseline_violations`): the rows hold one cleaner/chunker version and size, and those hash to
  the logged `config_id` (`chunk.config_id_of`); every row is what its chunk_id says (chunk_id recomputed from
  path, section path, embed text and occurrence; embed text = breadcrumb + body; n_tokens recounted); every row
  belongs to a doc of the baseline's bronze snapshot, with its commit and content hash; every bronze doc has
  rows unless it is a known zero-chunk doc; and (from `cfbf52e`) per doc, chunk_index runs 0..n-1, offsets
  never go backwards and one clean-text hash is named. Then the same pins as before: the commit, the docs
  fingerprint and the chunk fingerprint the baseline run logged.
- What the stored check cannot show is that the old versions chunked bronze into exactly these rows. The
  (path, chunk_id) set rests on the logged chunk fingerprint, which the baseline run computed when it re-chunked
  them under its own code and compared them with silver. Positions (chunk_index, offsets) are pinned by nothing,
  so a moved position on an old row would only change `chunks_kept_relocated`, which no verifier checks.
- A target under another config than its baseline needs `--allow-config-change`; without it the run is
  refused before any chunking. The flag is refused when the config does not change (from `cfbf52e`), so it
  cannot stand in a job's parameters and wave later version bumps through. The chunk diff across configs is the usual set difference by chunk_id, so a
  cleaner fix re-embeds only the chunks whose text changed; unchanged docs may now add or remove chunks
  (`chunks_added_unchanged_docs`, `chunks_removed_unchanged_docs`), which the same-config rule forbids
  otherwise. Drift compares the two configs' totals at the threshold for the release kind.
- The run log records how the baseline was checked (new column `baseline_check`: `rechunked` or `stored`).
- The verifiers re-check such runs with each run's own code: `scripts/oldcode.py` checks out the commit in its
  run-log `code_version` (commits with the same `src/` tree share one checkout) into a temporary git worktree
  and runs its chunker in a child process, which must compute the logged `config_id`.
- First real use: CLEANER_VERSION 2 drops a language tab's `<lang>:` label when the tab opens with a code block
  fenced in the same language (the label only repeated the fence, and between two pieces of a split block it
  was a tiny chunk of its own).

**Evidence** (2026-10-20, chain of sink `none`, head v4.0.1 under cleaner 1, config `ccbd80f05e15c8dd`):

| Run | What happened | File |
|---|---|---|
| k | `ADD COLUMNS baseline_check` (run log, 47 columns) | `m4x_k_*` |
| l1–l6 | silver under cleaner 2 for v3.5.0, v4.0.0, v4.0.1 at 254/32 (`e70bf55c7cf1b856`: 5,567 / 5,423 / 5,436 chunks) and 128/16 (`ece77f296d18fbd5`: 8,629 / 8,744 / 8,767); cleaner 1's rows stay beside them (`m4x_u`). l5's file records commit `7eccfd1` but its wheel was built from `7f5ffbe`, which only adds `runs/m4x_x` (a commit landed during the build); `run_job.py` now refuses that | `m4x_l*` |
| m | v4.0.1 under cleaner 2 without `--allow-config-change`: refused before chunking; `failed` | `m4x_m_*` |
| n | one cleaner-1 silver row of v4.0.1 (`8671aa66…`, `docs/index.md` #0) tampered: embed text 969 → 980 characters, chunk_id kept | `m4x_n_*` |
| o | v4.0.1 with the flag: the stored check names the row three ways (chunk_id does not match its content; embed text is not breadcrumb + body; n_tokens wrong); `failed`, `baseline_check` = stored (the run log row, `m4x_y`) | `m4x_o_*` |
| p | the row restored: the same sha256 and length as before the tamper | `m4x_p_*` |
| q | v4.0.1 with the flag: baseline checked from stored rows; docs 0 / 0 / 0; chunks 5,462 → 5,436, of which 221 added and 247 removed in unchanged docs and 5,215 kept; 42,372 of 691,882 tokens to embed (6.1%, against all of them for a new chain); drift within 5%; `committed`, `baseline_check` = stored | `m4x_q_*` |
| r | v4.0.1 again: the baseline is now under today's config, so re-chunked; 0 / 0 / 0, 0 tokens; `committed`, `baseline_check` = rechunked | `m4x_r_*` |

- *Independent checks:* silver under cleaner 2 equals a local chunking of git (`m4x_s_silver_vs_git.json`,
  36 checks, all pass). `m4x_v3_gate_vs_git.json` (1,089 checks) and `m4x_w3_changes_vs_git.json` (342 checks),
  all pass, re-check every gated and every logged run; cleaner-1 configs are re-chunked with each run's own
  code (14 commits, from M3's `9cde444e83` on), and each computes the logged `config_id`. For run q the verifier,
  with the baseline's code on one side and q's on the other, gets the logged 221 / 247 / 5,215 / 42,372, and it
  checks that q logged `stored` and r `rechunked`, the path each one's code had to take. An earlier pair of
  files (`m4x_v2`, 1,085; `m4x_w2`, 334; all pass) re-chunked every cleaner-1 run with one late commit's code,
  picked from an unordered query, which an audit caught; kept as history. Refusals before the gate (runs e, m,
  o) rest on the job's own error messages: the verifier checks only that they are not the gate's. Each job had
  one task attempt (`m4x_t`). Bronze has no history entry after 2026-10-15; silver's history shows the six
  MERGEs, one automatic OPTIMIZE between l5 and l6 (D-010), the tamper and the restore (`m4x_u`).
- *What the fix changed* (git only, every release from v3.0.0, `m4x_x_calibration_cleaner2.json` against
  `m4_a`): tiny chunks of v4.0.1 at 254/32 fell from 46 to 20, and at 128/16 from 101 to 24; across the 42
  releases the tiny share fell from 0.83–0.92% to 0.35–0.45%. Duplicate embed texts of v4.0.1 at 254/32 rose
  from 3 to 5 (at 128/16 they fell from 28 to 26): `m4x_q` logs a new group of three in the streaming guide.
  Seen in a local chunking of git, not saved: those three chunks are "See the full source code.", the end of
  three language tabs, and under cleaner 1 the next tab's label was packed into two of them, which kept them
  apart. Drift at the per-kind thresholds still fires on 3 of 41
  transitions.
- *Cost:* the re-baseline run took 61 s of job time, like any detect run (`m4x_q`).

**Consequences.**
- Old silver rows must stay while any chain's baseline points at their config; silver is immutable and never
  pruned today, which keeps this true. A future clean-up must check the heads of all chains first.
- The stored check recounts tokens with today's vocabulary; a vocabulary change (part of `config_id`) would fail
  it. A model with another tokenizer needs its own decision (M6).
- M5: a cleaner fix after gold exists goes through this path: the gold MERGE applies the cross-config chunk
  diff, and the index re-embeds only the changed chunks.

## D-014 — Embeddings into gold, and the AI Search index as a replica of gold

- **Date:** 2026-10-20 · **Status:** Accepted (evidence: `runs/m5_a`..`m5_p`, verified by `m5_v1`, `m5_v2`,
  `m5_v3b`)

**Context.** The brief's items 3 and 4: embed only changed chunks, remove the chunks of deleted docs, and load
the vectors into an index (AI Search Delta Sync with self-managed embeddings, D-001's option 1). Carried in:
- D-001: an endpoint bills 4 DBU/h even when idle; index capacity is sized at creation; "ready" is not
  "synced"; the sync pipeline's row metrics are 0.
- D-011: gold holds only what a chunk_id determines; gold must equal the baseline before a run changes it;
  MERGE counts must equal the diff.
- D-012: a planned order of steps, and the need for a recovery rule for a failure between the MERGE and the
  committed row.
- D-013: a cleaner fix reaches gold as a cross-config chunk diff.

**Decisions.**
1. *Model.* `sentence-transformers/all-MiniLM-L6-v2`:
   - pinned to revision `1110a243…`, the one M2's parity run checked;
   - 384 dimensions, vectors normalized, batch 64, on CPU. AI Search ranks by L2 distance, which on unit vectors
     gives the same order as cosine similarity.

   The job installs sentence-transformers 6.1.0, transformers 5.19.0 and the CPU build of torch 2.14.1 from the
   PyTorch index. The torch wheel is pinned by sha256 per machine, because from PyPI, M0 got the CUDA build. The
   job checks the installed versions at start (`Config.embedding.packages`). `name@revision` is stored on every
   gold row and committed row.
2. *Canary.* The wheel ships three fixed texts and the vectors the pinned model gave them on x86_64
   (`m5_a`). Every job that embeds encodes those texts first and stops, before the bulk encode, unless every
   cosine with the shipped vectors is at least 0.999. A wrong model, revision, pooling or normalization changes
   the vectors while every other vector check (dimension, finite values, unit norm) still passes. The cosines
   are logged as the `embedding_canary` DQ row: with the other four vector rows when it passes, alone when it
   fails (the job then stops). The alone-on-failure path was added after the demo runs (`860e3e1`) and is
   covered by unit tests only.
3. *Gold rows never change.* A run inserts the chunks it added and deletes the ones it removed; it never
   updates a row. A kept row keeps the `embedded_by_run_id` of the run that inserted it, and that is the
   evidence that only changed chunks were embedded. The schema is frozen once an index exists, because the
   index's schema is fixed at creation. A model change means a new gold table and a new chain, not an update.
   UPDATE therefore never reaches the index; M0 showed it would propagate.
4. *Order of steps; this changes D-012's.* In `jobs/change_detect.py`:
   1. chunk gate;
   2. gold state check: gold holds the chain head's chunks, all of the published model;
   3. embed the added chunks only;
   4. vector checks, written as the attempt's second DQ insert, which issue `VectorsPassed`;
   5. one MERGE, which needs both gates' passes; its own metrics and the history's must equal the diff;
   6. gold must equal the target;
   7. committed row.

   With no change there is no model load, no vector rows and no MERGE, because a no-op MERGE still writes a
   table version (D-006).

   The index is synced afterwards, by its own job. D-012 had the sync, and the wait for it, before the commit.
   The index is a replica of gold. If the sync came first and failed, gold would be merged while the run stayed
   uncommitted. That blocks the chain over a failure that changed nothing in gold: the next run finds gold off
   the chain head (at its own target if it repeats the tag, which decision 5 stops; at neither otherwise). Committing first keeps the chain's promise that gold holds the head's
   chunks, and a failed sync is simply run again.

   The cost: a committed head no longer says the index has caught up. Only the sync job's summary says that
   (the artifacts Volume and `runs/`; it names the chain head it synced to and writes no table row). Anything
   that reads the index (M6, M9) should run `sync_index` first: it is cheap when nothing changed (a key scan,
   no sync) and proves the index equals gold. This also replaces D-001's plan of syncing
   inside the pipeline job. The CDF retention window still argues for running the sync right after the commit,
   as M7's job graph will.
5. *No automatic resume.* Suppose gold already holds a run's target while the chain head is still its
   baseline: an earlier attempt merged but never committed. A resume would accept any write that happens to
   produce the target as this run's work. Instead the run stops. Gold is then put back by hand, with a
   compensating MERGE from the baseline's version, and the run is repeated. Two things support the repair:
   - The MERGE's version is recorded in the failed row as soon as the statement returns or raises, before any
     check.
   - Databricks' own maintenance commits (OPTIMIZE, VACUUM) are told apart from writes.
6. *The sync job (`jobs/sync_index.py`).* It syncs only a committed state: gold must equal the chain head's
   logged fingerprint. Then:
   1. It lists every key of the index (scan). If they equal gold's, the index is current and no sync is made;
      gold rows never change, so equal keys mean equal rows.
   2. Otherwise it makes one sync and waits for the update that call started to COMPLETE and for the index to
      report `ONLINE_NO_PENDING_UPDATE`.
   3. It lists the keys again, and requires them to equal gold's and `indexed_row_count` to equal gold's row
      count.
   4. Three probes query the index with gold rows' own vectors, and each row must come back in its own top 5.
      This checks the mechanics, not retrieval quality, which is M6.

   A failed index state stops every wait at once, because the endpoint bills by the hour. (In the demo runs
   only the final two waits checked it; the other two were added in `860e3e1`, unit-tested only.) The REST calls go
   through `api_client.do`, so the job runs with the serverless job's own credentials and the module tests run
   against a fake.
7. *A new chain.* Gold's chain starts with `--initial` at v3.5.0 and follows D-002's path. It is not compared
   with the `none` chain, whose head is v4.0.1. Starting at v3.5.0 is what demonstrates incremental embedding,
   and the first load has nothing to drift from. This answers D-012's open question.
8. *Lifecycle.* The endpoint and the index exist only while needed:
   - the index is created after the full backfill (capacity, D-001);
   - both are deleted afterwards;
   - gold stays, and the index can be rebuilt from it.

**Evidence** (2026-10-20, sink `gold_chunk_embeddings`, config `e70bf55c7cf1b856`, cleaner 2; each job ran
in one task attempt, `m5_o`):

| Run | What happened | File |
|---|---|---|
| a | canary made locally (x86_64, torch 2.14.1+cpu); the cache resolved the pinned revision | `m5_a_*` |
| b | run log `ADD COLUMNS` (embedding_model, chunks_embedded, embedded_ids_fingerprint, gold_rows_inserted, gold_rows_deleted, gold_version): 53 columns | `m5_b_*` |
| c | v3.5.0 `--initial`: gold created; 5,567 chunks (666,786 tokens) embedded in 193.1 s, 28.8 chunks/s on serverless aarch64, model load 11.8 s; canary cosines 0.99999 (aarch64 against the x86_64 reference); MERGE 5,567 inserted, gold version 1; job 390 s | `m5_c_*` |
| d | v4.0.0: drift_docs 244 → 212 (−13.1%, limit 10% for a major release) fails the gate before gold: no model load, no vector rows | `m5_d_*` |
| e | endpoint created: ONLINE 0.3 s after the call | `m5_e_*` |
| f | index created: `PROVISIONING_ENDPOINT` for 710–741 s (30 s polls), then index, pipeline resources and initial snapshot; `ONLINE_NO_PENDING_UPDATE` with 5,567 rows after 988 s. So the endpoint's ONLINE did not mean provisioned, as in M0 (D-001): the first index on a recreated endpoint waits again | `m5_f_*` |
| g | sync job: the index's 5,567 keys equal gold's, so no sync; 3/3 probes top-1 | `m5_g_*` |
| h | v4.0.0 accepting run d's drift: docs +19 / ~96 / −51; chunks 5,567 → 5,423; **1,866 embedded** (590 chunks in new docs, 1,276 in changed docs; 215,078 of 689,619 tokens, 31.2%); 3,557 kept, not re-embedded (2,037 moved: the job's own count, which no verifier checks); 2,010 deleted (1,484 from changed docs, 526 from deleted ones); gold version 2 | `m5_h_*` |
| i | sync job: the index was missing exactly 1,866 keys and held 2,010 extra; one update COMPLETED; then keys equal gold's (5,423); probes 3/3 | `m5_i_*` |
| j | v4.0.0 again: 0 added / 0 removed / 5,423 kept; no model load, no MERGE, no gold version | `m5_j_*` |
| k | v4.0.1 (patch, gate passes): **21 embedded** (3,538 of 691,882 tokens, 0.51%); 8 deleted; 5,415 kept; gold version 4. Version 3 is an OPTIMIZE Databricks ran on its own between h and k | `m5_k_*` |
| l | sync job: missing 21, extra 8; one update; then equal (5,436); probes 3/3 | `m5_l_*` |
| m, n | index deleted (gone after 15.7 s), then the endpoint | `m5_m_*`, `m5_n_*` |

- *Independent checks:*
  - `m5_v1_gold_vs_git.json` (42 checks, all pass) re-chunks git at each run's tag. For each run, gold at the
    version its MERGE wrote (time travel) holds exactly git's chunks, and every row's text columns equal the
    chunk's. The rows a run inserted are exactly the chunks added since the previous run. Every kept row
    still names the run that inserted it before.
  - Also in `m5_v1`: the logged counts, the embedded fingerprint (recomputed there) and the history's MERGE
    metrics agree. The history holds exactly one MERGE per run that changed gold. Every vector has 384 finite
    values, and the largest L2 norm error is 1.3e-7. A sample of 200 rows re-embedded locally (x86_64) has a
    minimum cosine of 0.99998 with gold's. The index's keys (scan) equal gold's.
  - `m5_v2_gate_vs_git.json` (379, all pass; again after the audit fixes as `m5_v2b`, 379) re-checks every DQ
    row, now including the five vector rows. Those rows exist exactly for runs that embedded; four have the
    added count as their denominator, the canary row its 3 texts. They were written in
    a second insert, after the chunk checks and before the committed row.
  - `m5_v3b_changes_vs_git.json` (85, all pass) re-checks the counts and the chain against re-chunked git and
    D-002.
  - An earlier `m5_v3` (93 checks) failed five of its eight comparisons with `runs/m2_e`. The verifier had
    matched `m2_e` by chunk size alone, but `m2_e` holds cleaner 1's numbers (config `ccbd80f05e15c8dd`), and
    this chain runs under cleaner 2. The verifier now matches by config id, computed with `m2_e`'s own
    commit's code. `m2_e` has no numbers for cleaner 2, so `m5_v3b` makes no `m2_e` comparison for this chain:
    its 85 checks are `m5_v3`'s 93 less those eight. `m5_v3` is kept as history. Before M5 no run had reached
    that comparison under cleaner 2.
- *Cost of incremental embedding:* the embedded share of tokens was 100% for the initial load, 31.2% for a
  major release and 0.51% for a patch release. The detect jobs took 101–390 s and the sync jobs 97–217 s.
  The endpoint existed from 18:26 to 19:09 UTC (about 43 min; at D-001's 4 DBU/h about 2.9 DBU, an inference).
  `system.billing.usage` had no rows for the window yet (`m5_p`). Read again on 2026-10-21 (`m6_l`): the
  hours 18:00 and 19:00 UTC hold 2.96 DBU of `VECTOR_SEARCH`, of which 1.84 is the endpoint
  (`REAL_TIME_INFERENCE`) and 1.13 the sync pipelines (`JOBS_SERVERLESS`). Jobs used 3.00 DBU and SQL 3.52 in
  the same hours. Rows can still arrive late, so these are the rows so far.
- *Review before the first run* (one agent): no definite bug. Four points were fixed:
  - the gold version is recorded before the post-MERGE checks;
  - maintenance commits are accepted beside the MERGE;
  - a failed index state ends the waits;
  - the canary runs before the bulk encode.

  Two points could only be settled by running, and the runs settled both: the pip markers on the serverless
  dependency lines, and the job's credentials for scan and query.

**Consequences.**
- The manual repair (decision 5) is untested: no run failed between a MERGE and its commit.
- Silver rows of a config must stay while a chain's head points at it (D-013). Gold needs no positions; they
  come from silver (D-011), and the job counted 2,037 kept chunks that moved in run h alone.
- Once a model change creates a second gold table, it needs a second index. The endpoint holds both, but
  quota caps how long they can stay up.
- M6 ranks the 2×2 grid without four live indexes (D-001), against gold-like tables of each model and size.
  It can reuse this pipeline's embedder. There is one canary file today (MiniLM): a second model first needs a
  canary file per model in `scripts/m5_canary.py` and `embedder.load_canary`.
- Still open from D-012: the override's reason test is lexical, nothing records who overrode, and gains are
  not capped. Embedding cost is now measured: 28.8–32.2 chunks/s on the two large encodes, about 3 minutes
  of encoding for a full load.

## D-015 — Retrieval evaluation: the eval set, the metrics, and the comparisons

- **Date:** 2026-10-21 · **Status:** Plan fixed before the first eval run (this section is committed before
  any result exists, so its git history shows the order); results to follow below it.

**The corpus.** Spark v4.0.1 under cleaner 2, chunked locally from git with today's code at both configs of
`Config.chunk_grid` (254/32: 5,436 chunks; 128/16: 8,767). Each local chunking's fingerprint must equal
silver's (`m6_b`, `m6_e`): the eval ranks the pipeline's own chunks.

**The set (v1).**
- Sections are drawn by `scripts/m6_sample_sections.py` (`m6_a`): one section per doc, from the 167 docs with
  a section of at least 300 characters of prose (stubs, zero-chunk docs, code and table lines excluded),
  shuffled with seed 6, the first 75.
- One agent drafted one item per section (question, short answer, a verbatim quote of the cleaned text that
  contains the answer, kind). A second, skeptical agent reviewed each against the section and the whole
  cleaned corpus: 49 kept, 16 fixed, 10 dropped (`eval/drafts_v1.json`).
- The set takes the first 50 usable items in sample order; the other 15 are spares, used in the same order
  for any item the owner rejects. So which sections the set covers was fixed before any retrieval ran.
- Checked (`m6_e`): every quote occurs exactly once in the corpus and inside its labelled section, every item
  has 1–2 relevant chunks under both configs, and no question shares a run of more than 3 words with its
  quote.
- 8 items state their answer in another doc too, in other words (`also_in`). Retrieving that doc counts as a
  miss, so a sensitivity check reports the set without them.
- The owner reviews every item before the run (a review page; the decisions are saved to `runs/`).

**Definitions.** `Config.eval`, `docs_rag.evaluate`:
- *Relevant:* a chunk whose slice of the cleaned text covers at least half of an occurrence of the quote. The
  label is the quote, not chunk ids, so one label serves both chunk sizes (the open question of D-010).
- *recall@5:* the share of items with a relevant chunk in the top 5.
- *MRR@10:* the mean of 1/rank of the first relevant chunk, 0 beyond rank 10.
- *Search:* exact cosine over all chunks (normalized vectors, the full ranking), not the ANN index: the grid
  cannot have four live indexes (D-001). For the published config the eval also ranks with gold's own vectors,
  which must give the same ranks.
- *Uncertainty:* 10,000 bootstrap resamples of the 50 items, seed 15, percentile 95% intervals. Differences
  are paired: every config uses the same resamples.

**Configurations.** all-MiniLM-L6-v2 and bge-small-en-v1.5, at their pinned revisions, × 254/32 and 128/16,
embedded on one machine's CPU. bge's queries carry the model card's instruction for short queries against
passages ("Represent this sentence for searching relevant passages: "); its sentence-transformers config sets
none (`m2_f`).

**Comparisons, named before the run.**
1. Chunk size within each model: 254/32 against 128/16.
2. Model within each chunk size: bge (with the instruction) against MiniLM.
3. bge's instruction within each size: with it against without it.
4. The published config with gold's vectors against the same config with local vectors. This should be zero.
5. The published config through the deployed AI Search index (approximate search, `--index`) against exact
   search over the same gold vectors, plus how much of each question's exact top 10 the index returns. This
   needs the endpoint up for one window (about an hour, D-001). The owner chose this on 2026-10-21, while the
   results were still unknown: the eval then measures the serving path the pipeline syncs, not only an
   offline copy.

**Decision rule.** With n = 50, a difference counts only if its paired 95% interval for recall@5 excludes zero.
MRR@10 is reported beside it and breaks no tie. If no comparison clears that bar, production keeps what it has
(MiniLM at 254/32, D-014), because it is the cheaper model and the config that is already embedded. A result
the set cannot resolve is reported as such, not as a winner. The `also_in` sensitivity check (the set
without those 8 items) and the split by doc area (SQL reference pages, 23 items, against the rest) are
descriptive only. Not compared, by the owner's choice: section-aware against whole-page chunking (D-009's
open question), which stays a stated limitation.

**Owner's review** (2026-10-21, `eval/review_v1.json`): all 50 items kept, none rejected or edited, so the
spares stay unused. The review page's store recorded no clicks; the owner confirmed the decision in chat.

**Results** (`runs/m6_i_eval_v1.json`, 2026-10-21; set v1, sha256 `b19d98fe…`; 400 rows in `eval_results`,
`eval_run_id` `m6_i_eval_v1-fb215b54`):

| Config | recall@5 [95% CI] | MRR@10 [95% CI] |
|---|---|---|
| MiniLM 254/32 (production) | 0.68 [0.54, 0.80] | 0.486 [0.378, 0.597] |
| MiniLM 254/32, gold's vectors | 0.68 [0.54, 0.80] | 0.486 [0.378, 0.597] |
| MiniLM 254/32, the live AI Search index | 0.68 [0.54, 0.80] | 0.489 [0.381, 0.600] |
| MiniLM 128/16 | 0.72 [0.60, 0.84] | 0.500 [0.390, 0.612] |
| bge-small 254/32, with the instruction | 0.74 [0.62, 0.86] | 0.581 [0.464, 0.696] |
| bge-small 128/16, with the instruction | 0.74 [0.62, 0.86] | 0.607 [0.485, 0.727] |
| bge-small 254/32, without | 0.74 [0.62, 0.86] | 0.531 [0.421, 0.644] |
| bge-small 128/16, without | 0.70 [0.56, 0.82] | 0.564 [0.442, 0.683] |

The comparisons named in the plan. Each is the paired difference a − b with its 95% interval; "items" counts
the questions only a found in the top 5 against those only b found.

| # | a vs b | recall@5 | items | MRR@10 |
|---|---|---|---|---|
| 1 | MiniLM 254/32 vs 128/16 | −0.04 [−0.12, +0.04] | 1 vs 3 | −0.014 [−0.075, +0.043] |
| 1 | bge 254/32 vs 128/16 | 0.00 [−0.06, +0.06] | 1 vs 1 | −0.027 [−0.093, +0.040] |
| 2 | bge vs MiniLM at 254/32 | +0.06 [−0.06, +0.18] | 6 vs 3 | **+0.095 [+0.009, +0.186]** |
| 2 | bge vs MiniLM at 128/16 | +0.02 [−0.08, +0.12] | 4 vs 3 | **+0.108 [+0.034, +0.185]** |
| 3 | bge with vs without the instruction, 254/32 | 0.00 [−0.06, +0.06] | 1 vs 1 | **+0.049 [+0.013, +0.094]** |
| 3 | the same, 128/16 | +0.04 [0.00, +0.10] | 2 vs 0 | **+0.043 [+0.001, +0.089]** |
| 4 | gold's vectors vs local, production config | 0 (identical ranks) | 0 vs 0 | 0 |
| 5 | live index vs exact search, gold's vectors | 0.00 [0.00, 0.00] | 0 vs 0 | +0.003 [0.000, +0.010] |

- **Decision, by the rule fixed in advance:** no recall@5 interval excludes zero. The instruction at 128/16
  comes closest, but its interval starts at zero: 2 items against 0, which the set cannot tell from chance. So
  production keeps MiniLM at 254/32.
- **What the set does show:** MRR@10 favours bge-small with its instruction at both sizes, and its intervals
  exclude zero. When both models find the answer, bge tends to rank it higher. By the rule, MRR breaks no tie.
- **Cost of switching to bge:** a new gold table and chain (D-014), at about half MiniLM's embedding speed
  (33.8 against 64.7 chunks/s here at 254/32). A larger set that can resolve recall should come first.
- **The serving path loses nothing measurable.** Gold's vectors rank exactly like the local ones; their lowest
  cosine is 0.99997. The live index returned 99.8% of exact search's top 10 per question, with identical top-10
  lists for 48 of the 50, the same recall@5, and an MRR@10 0.003 higher (one item ranked differently). So the
  numbers for the production config hold for the deployed index, not only for an offline copy.
- **Descriptive:**
  - *By doc area:* SQL reference questions reach recall@5 of 0.61–0.70 across configs, the rest 0.70–0.85.
    bge's lead is entirely outside the SQL reference: 0.85 against MiniLM's 0.70 at 254/32, while on the SQL
    reference it has 0.61 against 0.65.
  - *Without the 8 `also_in` items* (n = 42): the order stays the same, 0.69 / 0.69 / 0.74 / 0.74 for MiniLM
    254, MiniLM 128, bge 254, bge 128.
  - *Misses:* 28 questions are found by every config and 9 by none, 6 of those 9 on SQL reference pages. Those
    pages are many short docs built from one template (Description, Syntax, Parameters, Examples), so a question
    competes with its siblings' near-identical chunks. For three of the nine (q09, q11, q28) the relevant chunk
    ranks beyond 400 under every config. That suggests the answer sentence sits in a chunk dominated by other
    text; it has not been checked.
- **Cost:** the endpoint was up for 28 minutes (03:33–04:01 UTC, `m6_f`..`m6_k`). Local embedding on 4 vCPU
  x86_64 ran at 64.7 and 113.6 chunks/s for MiniLM (254/32, 128/16) and 33.8 and 57.9 for bge. The whole run
  took about 10 minutes. Billing, read later that day (`m7_i`): the 03:00 and 04:00 UTC hours hold 1.33 DBU of
  `VECTOR_SEARCH`, 0.90 of it the endpoint (`REAL_TIME_INFERENCE`, 0.59 + 0.31) and 0.43 the sync pipelines.
  At D-001's 4 DBU/h, 28 minutes would be about 1.87 DBU; like M5's window (1.84 for 43 minutes, D-014), a
  short window billed less than that rate. Why is not known.

**Consequences.**
- Production is unchanged (D-014). Switching models stays open until a larger set can resolve recall. By the
  normal approximation of the plan, a ±5-point interval needs about four times the items, roughly 200; that
  is an inference, not a measurement.
- The SQL reference is where retrieval fails most. A follow-up would be a page-level signal (the breadcrumb
  is already in each embed text) or hybrid lexical search; neither is tested.
- Section-aware against whole-page chunking is still unmeasured (D-009).
- The eval harness reuses the pipeline's chunker, embedder and the gold table, and ranks the deployed index
  with the same labels. A re-run after a corpus or model change is one command.

**Set v2 at v4.2.0** (2026-10-21; this paragraph is committed before v2's first run). M7 moved dev's gold chain
to v4.2.0 (D-016), so comparison 4 (gold's vectors against local) refuses set v1, which is labelled at v4.0.1.
- *The set* (`eval/eval_set_v2.json`): v1's 50 owner-reviewed items at v4.2.0, with questions, answers,
  sections and `also_in` unchanged. One quote changed with the docs: in q19's sentence v4.2.0 writes NULL in
  backticks, so its quote is the release's text for the same sentence (the change is recorded in the set).
  A local check against git found every quote once, inside its section, and in no other doc. `also_in` was not
  re-reviewed for docs added since v4.0.1.
- *What it is for:* comparison 4 at the head of dev's chain, and the same configs and comparisons on a later
  release, read with v1's decision rule. No new comparison and no decision depends on it, and v1's results
  stand. Not run: the live index (it needs the endpoint, D-001).

## D-016 — Orchestration and deployment: one bundle, one job, dev and prod

- **Date:** 2026-10-21 · **Status:** Accepted (evidence: `runs/m7_*`)

**Context.** Brief item 7: Databricks Jobs deployed with Asset Bundles, dev and prod targets, and a GitHub
Actions workflow that runs the tests. Carried in: one concurrent run per job (D-007, D-011), no task retries
without a per-attempt key (D-011), a failure notification for a scheduled run that the gate stops (D-012),
serverless environment 4 pinned (D-005), the index synced only from a committed state (D-014).

**Decisions.**
1. *One bundle (`databricks.yml`, CLI 1.21.0), two targets.* `dev` (development mode: resources named
   "[dev <user>] ...", deployed under the user's home, schedules paused) on `docs_rag_dev`; `prod` (production
   mode) on `docs_rag_prod`, run as the deploying user. A service principal is the usual prod identity; this
   project has none (Free Edition, not checked whether one can be made), so prod runs as the owner.
2. *One job, the existing scripts as tasks* (`resources/pipeline.job.yml`): resolve_tag → bronze → silver
   254/32 → silver 128/16 → change_detect → index_requested (condition) → sync_index. No pipeline logic moved:
   each task writes the same summary JSON as a `run_job.py` run, so the M3–M5 verifiers check orchestrated runs
   unchanged (`scripts/bundle.py run` writes the detect task in run_job's shape).
3. *The tag.* A job parameter, `latest` by default: resolve_tag reads the newest plain release from git's
   smart-HTTP ref advertisement (no API rate limit) and hands it to the other tasks as a task value, so every
   task runs on one tag even if a release lands mid-run. A weekly schedule (Mondays 06:15 UTC) therefore picks
   up a new Spark release by itself; it is paused in both targets, because each run spends quota: unpausing it
   is the owner's call.
4. *Switches are job parameters* (`initial`, `allow_backwards`, `allow_config_change`, `accept_drift`,
   `accept_drift_of`, `with_index`): the scripts accept `--flag=true|false` and an empty text as "not given"
   (`docs_rag.jobenv.add_flag`, `optional_text`), and keep working from `run_job.py`. The gate's override is a
   rerun naming the failed run, whose id is "<job run id>-detect".
5. *Index sync is opt-in* (`with_index`, default false): the endpoint exists only while needed (D-001, D-014),
   so a scheduled run updates gold and a condition task leaves the sync out.
6. *Deployment pins the code.* `scripts/bundle.py deploy` refuses uncommitted code, builds the wheel, and
   deploys with its version as a bundle variable; every task checks it imports exactly that version (D-005).
   The variable's default fails closed. The embedding packages are the same lines as `run_job.py`'s
   (`docs_rag.deps`), and `tests/test_bundle.py` checks the YAML against the code (environments, scripts,
   parameters, concurrency, paused schedule) in CI without a workspace.
7. *Data stays outside the bundle.* The schema and the artifacts Volume are made by `scripts/bundle.py`
   (as by `run_job.py`) and tables by the jobs, so `bundle destroy` removes a job and its files, never data.
8. *Concurrency.* `max_concurrent_runs: 1` (and the same preset in both targets), no retries on any task. The
   two silver tasks first ran side by side and Delta aborted one MERGE (finding below), so they run one after
   the other.
9. *Failure email* to the deploying user (`${workspace.current_user.userName}`, resolved at deploy, not in the
   repo). Dev resource names carry the user name's local part, which `scripts/redact.py` now masks too.
10. *CI/CD.* `ci.yml` (lint, types, tests incl. the bundle tests, the corpus golden) is reusable; `deploy.yml`
   runs it, then deploys dev on every push to main, and dev or prod on demand, in a GitHub environment named
   after the target (prod should require a reviewer). Actions are pinned to commit SHAs. A push that changes
   only run records, the eval set, docs or Markdown skips the deploy, so a commit of evidence does not deploy a
   new wheel version of the same code (added after the first run; `tests/test_bundle.py` checks that no
   deployed path is ignored). The workspace credentials are repository or environment secrets the owner
   sets; a personal access token is the only credential tested here (OAuth M2M or GitHub OIDC federation would be better; not checked on Free Edition).

**Evidence** (2026-10-21; dev runs on the gold chain of `docs_rag_dev`, head v4.0.1 before them):

| Run | What happened | File |
|---|---|---|
| a | dev deployed (5.0 s; wheel `0.0.1.dev187+g6f75214b3`) | `m7_a_*` |
| b | dev job, `tag=latest`: resolve_tag read 97 release tags and chose v4.2.0. Bronze and silver_254 ingested it. The gate stopped change_detect before gold, on all three drifts against the 10% of a minor release: docs 213 → 238 (+11.7%), chunks 5,436 → 6,051 (+11.3%), tokens 691,882 → 775,772 (+12.1%). The job failed, which sends the failure email (its arrival not checked here). silver_128, run beside silver_254, lost a write conflict (below) | `m7_b_*` |
| c | dev redeployed with the silver tasks in sequence | `m7_c_*` |
| d | dev job, `tag=v4.2.0` accepting run b's drift (`accept_drift_of=<b's job run>-detect`). Bronze and silver_254 were skipped as identical; silver_128 inserted 9,852 rows. Docs +25 / ~57 / −0. **802 chunks embedded** (109,328 of 775,772 tokens, 14.1%), 5,249 kept, 187 deleted; gold version 5. The sync was left out by its condition; the job took 308 s | `m7_d_*` |
| e | dev job, `tag=latest` again, as the schedule would run it: every step skipped, nothing embedded, no gold version (219 s) | `m7_e_*` |
| f | prod deployed (6.3 s): job `docs_rag_pipeline`, schema `docs_rag_prod` | `m7_f_*` |
| g | prod job, `tag=latest initial=true`: v4.2.0 into an empty prod schema; 6,051 chunks embedded in 221 s (27.4 chunks/s); gold version 1; 455 s | `m7_g_*` |
| h | GitHub Actions, on the merge of M7 into main: `deploy.yml` run [39594001897](https://github.com/m3rcury02/spark-rag/actions/runs/39594001897), checks 47 s, dev deploy 24 s. Dev then pins main's commit (`0.0.1.dev200+gcbf16517e`); prod was untouched | `m7_h_*` |
| j | GitHub Actions, prod by `workflow_dispatch`: run [39598227251](https://github.com/m3rcury02/spark-rag/actions/runs/39598227251), checks 60 s, deploy 23 s; the push of the same commit deployed dev ([39598196803](https://github.com/m3rcury02/spark-rag/actions/runs/39598196803)). Both jobs then pin `0.0.1.dev203+gd0871f013`. The prod deploy started 4 s after its checks and the run has no approvals: the `prod` environment required no reviewer | `m7_j_*` |
| k | the prod job as Actions deployed it, `tag=latest`: v4.2.0 again, a no-op (238 docs unchanged, 6,051 chunks kept, 0 embedded, no gold rows deleted); 252 s | `m7_k_*` |

- *Checks:* every orchestrated run passes the verifiers written for the `run_job.py` runs.
  - dev: gate 599/599 (`m7_v1`, the M5 and M7 runs), changes 126/126 (`m7_v2`), gold against git 53/53 (`m7_v3`, incl. run d: only its 802 added chunks carry its run id).
  - prod: gate 82/82, changes 22/22, gold against git 18/18 (`m7_v4`..`m7_v6`); after run k, 152/152,
    42/42 and 22/22 (`m7_v7`..`m7_v9`).
  - CI (lint, types, tests incl. `tests/test_bundle.py`, the corpus golden) passed on every M7 commit, e.g. [39579282118](https://github.com/m3rcury02/spark-rag/actions/runs/39579282118).
- *Finding: two writers to silver conflict.* In run b, silver_128 failed with `ConcurrentAppendException`
  (`DELTA_CONCURRENT_APPEND.WHOLE_TABLE_READ`): its MERGE reads the whole table, so a concurrent MERGE by
  silver_254 conflicts even though the two write different (config_id, tag) rows. The failed MERGE committed
  nothing, and run d inserted those rows. Options: partition or cluster silver by config_id so the reads are
  disjoint, or serialize the tasks. Chosen: serialize. It costs about 90 s a run, and nothing else needs
  parallel writes.
- *CI/CD:* both paths of `deploy.yml` ran green (runs h and j), and the job Actions deployed to prod ran (k),
  so a wheel built on a GitHub runner works on serverless as one built here does.
- *Finding: the prod deploy was not gated.* The `prod` GitHub environment existed but required no reviewer at
  run j, so the dispatch deployed prod with no approval. Deploying changes no data (decision 7), but anyone who
  can run workflows can change prod's code. The fix is a setting, not code: a required reviewer on the
  environment (Settings > Environments > prod); not re-checked after.

**Consequences.**
- One command per environment: `scripts/bundle.py deploy|run --target dev|prod`. A schedule, a person and CI
  all go through the same job, and its runs are checked like any other.
- Dev's chain head is now v4.2.0, while the M6 eval set v1 is labelled at v4.0.1, so its comparison with
  gold's vectors refuses. Set v2 carries the same items at v4.2.0 (D-015); v1's results stay as they are.
- Prod holds only v4.2.0 (one initial run): its chain and gold are independent of dev's. Prod has no eval
  results and no index; creating an index for it follows D-014 (after the backfill; delete it when idle).
- Still open: a scheduled run that the gate stops fails every week until someone accepts the drift (D-012).
  The failure email is the signal; failing fast on a repeat of the same failed target is not built.
- Every push to main that touches deployed code redeploys dev; prod changes only when someone dispatches it,
  and until the `prod` environment requires a reviewer, nothing stands between a dispatch and prod. A token stored in GitHub is a long-lived credential: use one made for CI, with an expiry, and
  prefer OAuth or OIDC federation where the workspace allows it.

## Open questions (decide in the milestone noted)

- **M2/M5 — chunk size vs model context.** *Answered in M2 (D-009):* sizes are counted in the models' own
  WordPiece units, and the parity run shows no chunk of either config over either model's limit. M4 keeps the
  check as a DQ rule (D-012): a chunk is oversized when its token count (the larger of a recount of the embed
  text and `n_tokens`) exceeds the smaller of the config's `max_tokens` and the model's `max_seq_length` less
  [CLS] and [SEP]; `Config` refuses a chunk config that could exceed the model's limit.
- **M3 — chunk ID stability.** *Answered in M2 (D-009, D-010):* content-addressed IDs within heading sections
  are local (an edit to a section's text leaves other sections' chunks alone; a heading rename re-IDs the
  chunks beneath it), but they do not cut re-embedding cost against a whole-page baseline. M3 counts chunk-level change as added and removed chunk_ids.
- **M4 — row-count drift threshold.** *Answered in M4 (D-012); the owner chose 2026-10-20:* drift of docs,
  chunks and tokens against the baseline run, at 5% for a patch release and 10% for a minor or major one (the
  same 3 firings in 41 historical transitions as one 10% rule, tighter on patch releases), with a logged
  override bound to a failed run's drift (`--accept-drift-of`). Built as gate v2 (`runs/m4x_*`).
- **M6 — relevance labels must not depend on chunking.** Gold answers are labelled as
  (doc path, section) plus an answer span, given as a verbatim quote of the cleaned text (D-010). A retrieved
  chunk counts as relevant if its slice overlaps an occurrence of the quote. Otherwise chunk sizes cannot be compared. With n=50, the 95% CI half-width on recall@5 is
  ±8.3 / ±11.1 / ±13.9 points at recall 0.9 / 0.8 / 0.5 (normal approximation, 1.96·√(p(1−p)/50)),
  so we report bootstrap CIs and paired differences, not just point estimates.
