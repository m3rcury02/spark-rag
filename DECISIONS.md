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
| Endpoint | `docs_rag_endpoint` (STANDARD) reported ONLINE 0.1 s after it was created. The first index then waited in `PROVISIONING_ENDPOINT` for at least 1043 s. Whether later index creations wait again is unknown | `endpoint`; `m0_smoke_test_cells.txt` cell 12 |
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
  `PROVISIONING_ENDPOINT`. Not yet checked: that the 96 DBU a day actually stopped (re-probe
  `system.billing.usage` more than 24 h after the deletion).
- Unity Catalog keeps dropped tables recoverable with `UNDROP` and counts them when checking whether a schema
  is empty. A plain `DROP SCHEMA` therefore failed with "not empty" while `SHOW TABLES` returned nothing, and
  the cleanup had to force-drop after checking `SHOW TABLES DROPPED` held only the two smoke-test objects
  (`runs/m0_cleanup.json`). Tearing down a dev schema (M7) needs the same check.
- Create the production index once, after `gold_chunk_embeddings` holds the full first backfill (v3.5.0),
  so its capacity is sized for the corpus. If capacity errors still appear, recreate the index rather than
  trying to grow it.
- Call `index.sync()` in the same job right after each gold MERGE, wait for that update to complete, and log
  its pipeline update id. This keeps the CDF gap short. CDF records "are transient and only accessible for a
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
  become a `<lang>:` label line.
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
retrieval-quality question for M6's recall@5/MRR, and changing one later is a version bump, not a redesign.
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

- **Date:** 2026-10-20 · **Status:** Accepted (evidence: `runs/m4_*`)

**Context.** Brief item 5: "Data-quality checks: empty or tiny chunks, oversized chunks, duplicate chunks,
missing metadata, and row-count drift between runs. A failed check stops the pipeline." CLAUDE.md: a failed
check stops the run before anything is published (write-audit-publish); results go to `ops_dq_results`. D-011
put the gate inside `jobs/change_detect.py`, before the committed row. The open question from M2: v3.5.0 →
v4.0.0 legitimately drops 13% of the docs, so a hard drift check fires on real releases unless it has a
threshold and a logged override.

**Options.**
- *Where the gate runs.* A separate job between silver and gold would split "committed" and "gated" across two
  jobs, so a commit without a gate would only be caught after the fact. Chosen: inside the change-detection job,
  after the diff and before the committed row, which can be appended only with the gate's `GatePassed`; M5's
  gold write goes in the same place and needs the same token.
- *What the checks cover.* Only the added chunks (what M5 embeds), or all of the target's chunks. Chosen: all,
  because gold holds the whole target and a check on the change set misses a defect already in the baseline.
  It is cheap next to re-chunking two tags, which the job already does (run times below).
- *Thresholds.* Relative to the baseline (a share that may not grow) or absolute shares. Chosen: absolute
  shares of the target's chunks, with room above what the real corpus has (tiny: 2% against at most 0.92% at
  254/32 and 1.17% at 128/16; duplicate embed texts: 1% against at most 0.43%); a relative rule lets a slow
  regression creep in one release at a time.
- *Drift.* (1) Hard fail, no override: fires on every big release and gets switched off (the open question).
  (2) Warn only: nobody reads warnings, and the brief says stop. (3) Per-release-kind thresholds (patch 5%,
  minor and major 20% would have fired on none of the 41 transitions since 3.0): rejected as a rule fitted to
  41 points, with one more concept to explain. (4) **One threshold plus a bound, logged override** (chosen).
- *Statuses.* A "warn" status was dropped: a check passes or fails; `overridden` and `not_applicable` exist
  only for drift.
- *Language-tab labels.* 8 of the 10 tiny chunks logged as examples for v4.0.1 are lone tab labels (`java:`,
  `scala:`, `r:`) left by the cleaner; the other two are one-item lists. Fixing them means a cleaner version
  bump and a silver re-run. Chosen: leave them for M6 (they are under the threshold and listed as examples on
  every run), and fix them there if retrieval shows they matter.

**Decision.** `docs_rag.dq` (pure Python; thresholds in `Config.dq`), called by `jobs/change_detect.py`:

| Check | Fails when | Threshold |
|---|---|---|
| `empty_chunks` | a body has no WordPiece token (blank, or only characters the tokenizer drops), or an embed text is blank | any |
| `tiny_chunks` | bodies under 5 tokens, as a share of all chunks | > 2% |
| `oversized_chunks` | an embed text (recounted, not read from `n_tokens`) over `min(max_tokens, 256 − 2)` | any |
| `duplicate_chunk_ids` | rows beyond the first of a repeated chunk_id | any |
| `duplicate_embed_text` | copies beyond the first of a repeated embed text, as a share | > 1% |
| `missing_metadata` | a path outside `docs/*.md`, a blank section name (title first), another `version_tag`, a commit that is not 40 hex | any |
| `drift_docs`, `drift_chunks`, `drift_tokens` | the target's total against the baseline run's: `|to − from| > 10% of from` | 10%; a loss over 50% cannot be overridden |

- **Override.** `--accept-drift "<reason>"` turns a drift failure into `overridden` for that run only. It is
  refused before the Spark session starts when the reason is under 20 characters or 4 words, or with
  `--initial`; refused by the gate when no drift check is over its threshold ("nothing to accept"); and it never
  covers a loss over 50% or any non-drift check. The reason is stored on every overridden `ops_dq_results` row
  and in the new run-log column `dq_override` of the committed row.
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
  `docs_rag.dq`): counts, observed values, thresholds, statuses, override reasons; each run's outcome and the
  checks its error names; and, from Delta's own history (time travel over each table's WRITE versions), that
  each run's nine DQ rows went in one insert before its run-log row. Run d2 has no DQ or log row. Every
  committed run since the gate has a complete passing set; the five M3 commits have none, and their code
  predates the gate commit. Before it ran on real data the verifier was tested on simulated tables, where a
  changed count, a missing `dq_override` and DQ rows written after the commit each failed it.
  `m4_w_changes_vs_git.json` re-runs M3's verifier over all 19 log rows: 184 checks pass, and the nine
  committed rows form one chain (v3.5.0 → 4.0.0 → 4.0.0 → 4.0.1 → 4.0.1 → 4.0.1 → 3.5.0 → 4.0.0 → 4.0.1).
- *Write-audit-publish holds without trusting the job:* bronze's last history entry is 2026-10-15 and silver's
  an `OPTIMIZE` of 2026-10-19, before M4 (`m4_k_*`); `ops_dq_results` has seven WRITEs of nine rows each, and an
  `UPDATE` of it is refused with `DELTA_CANNOT_MODIFY_APPEND_ONLY` (`m4_l_*`). Each run had one task attempt
  (`m4_j_*`).
- *Cost.* Detect runs took 56–83 s of job time (c: 104 s, including creating `ops_dq_results`; d2: 36 s),
  against 52–65 s in M3 (D-011; `run_duration_seconds`). The gate's own share was not
  measured separately.

**Consequences.**
- M5: the gold MERGE takes the same `GatePassed`, after the gate and before the committed row. Vector checks
  (dimension, no NaN, unit norm) join the gate as checks before the index sync.
- M5's first run is `--initial` on a new chain, so its drift is not applicable: the first gold load is compared
  with nothing. M5 decides whether to compare it with the head of the `none` chain.
- The thresholds are a policy calibrated on 42 releases of one corpus; another corpus needs its own
  calibration run (`scripts/m4_calibrate.py`).
- M7: the override is a CLI flag on one run. A scheduled job has no one to type it, so a drift failure there
  stops the schedule until someone runs the release by hand with a reason, which is the intended behaviour.

## Open questions (decide in the milestone noted)

- **M2/M5 — chunk size vs model context.** *Answered in M2 (D-009):* sizes are counted in the models' own
  WordPiece units, and the parity run shows no chunk of either config over either model's limit. M4 keeps the
  check as a DQ rule (D-012): a chunk is oversized when its recounted embed text plus [CLS] and [SEP] exceeds
  the embedding model's `max_seq_length`, and `Config` refuses a chunk config that could produce one.
- **M3 — chunk ID stability.** *Answered in M2 (D-009, D-010):* content-addressed IDs within heading sections
  are local (an edit to a section's text leaves other sections' chunks alone; a heading rename re-IDs the
  chunks beneath it), but they do not cut re-embedding cost against a whole-page baseline. M3 counts chunk-level change as added and removed chunk_ids.
- **M4 — row-count drift threshold.** *Answered in M4 (D-012):* 10% of docs, chunks and tokens against the
  baseline run, with a per-run `--accept-drift "<reason>"` that is logged, refused when nothing drifted, and
  never covers a loss over 50%. Over the 41 release transitions since v3.0.0 it fires on 3 (no patch release,
  2 of 7 minor, the one major).
- **M6 — relevance labels must not depend on chunking.** Gold answers are labelled as
  (doc path, section) plus an answer span, given as a verbatim quote of the cleaned text (D-010). A retrieved
  chunk counts as relevant if its slice overlaps an occurrence of the quote. Otherwise chunk sizes cannot be compared. With n=50, the 95% CI half-width on recall@5 is
  ±8.3 / ±11.1 / ±13.9 points at recall 0.9 / 0.8 / 0.5 (normal approximation, 1.96·√(p(1−p)/50)),
  so we report bootstrap CIs and paired differences, not just point estimates.
