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
  updated 2026-09-29). Jobs and SQL kept running on 10-15 with the endpoint's 96 DBU on the books, so the
  quota was not exceeded that day; whether endpoint usage counts towards it at all is unknown.
  **On 2026-10-19 the workspace refused all compute**, from 05:27Z: a job submit got "Triggering new runs for
  organization <workspace-id> is currently disabled temporarily" (`runs/m2_d0b_*`) and a SQL statement "The
  request could not be processed by the warehouse" (`runs/m2_d0_*`), while the status page showed all
  services operational. Nothing of ours had run that day; the endpoint had been billed about 96 DBU a day
  since 10-15. A community answer (not Databricks documentation) reads this message as the Free Edition quota
  being used up. If so, the quota is not simply per day, since 10-15 and 10-18 ran after more endpoint usage
  than 10-19 had by 05:27Z. Inference, not proof: the quota is not visible. The endpoint was deleted at
  05:26Z, a minute before the first refusal; that the deletion caused the refusal is unlikely but not ruled
  out. Lesson for M5–M6: create the endpoint only for the hours an index is needed, then delete it.
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
  delete). It was kept at first because Free Edition allows only one, but idle it cost 96 DBU a day of an
  undisclosed quota (see Quota) and nothing needs it before M5. M5 creates it again (STANDARD, same name)
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

- **Date:** 2026-10-19 · **Status:** Accepted (local evidence in `runs/m2_e_stability.json`; the silver-job
  residue figures are pending, see D-010 Status)

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
- **Code is masked first** (CommonMark fences, `<pre>`/`<xmp>`, inline spans), because code is full of `<...>`
  that is not HTML. A **conservation check** raises if masked code is missing from the output, except code
  inside markup that is dropped on purpose (HTML comments, `<style>`/`<script>`, image alt text, reference
  definitions). Cleaning may move code, never lose it.
- **HTML.** Only real tag names are stripped (an allowlist, matched to a name boundary, so `<col-name>` in prose
  stays). `<style>`/`<script>` go with their content. Tables are rendered innermost first as GFM tables (header
  and `---` delimiter when the first row is all `<th>`): `rowspan` cells are repeated into the rows they cover,
  `colspan` keeps the column count, a caption becomes a line above, list items in a cell are joined, a nested
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

**Evidence** (`runs/m2_e_stability.json`, 254/32, v4.0.1; the `ablations` key reruns the corpus with one rule
off):
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

**Context.** The brief asks for section-aware chunking with overlap and stable chunk IDs; M6 compares 2 chunk
sizes × 2 models. all-MiniLM-L6-v2 truncates at 256 word-pieces and bge-small-en-v1.5 at 512
(`max_seq_length` in the parity run). Both ship the same vocab.txt (sha256 `07eced37…`, recorded per model in
the parity run), vendored in `src/docs_rag/data/`.

**Options.** (1) fixed-size windows over the page; (2) greedy packing over the whole page (the "whole-page
baseline" below: same cleaner and packer, the page is one section, the breadcrumb is the title); (3) greedy
packing within heading sections (chosen); (4) content-defined boundaries.

**Decision.**
- Sizes are in **the models' own units**: WordPiece counts from the vendored vocab (`tokenizers==0.23.3`, pinned
  and checked by the job). `max_tokens` bounds the whole embed text `<breadcrumb>\n\n<body>`, with breadcrumb
  `title > h1 > h2 ...`; every `n_tokens` is a recount. Grid for M6: **254/32 and 128/16**: both fit MiniLM's
  256 positions with [CLS] and [SEP], with the same 12.5% overlap. 254 is beyond the 128 tokens MiniLM was
  trained on ("The sequence length was limited to 128 tokens", its
  [model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2), read 2026-10-19); whether that
  hurts is what the 128 config tests.
- Units are paragraphs, GFM tables and fenced code blocks. A unit that fits stays whole. One that does not is
  split: prose at sentence ends first (its line breaks are the source's hard wrapping; "e.g."/"i.e." do not end
  a sentence), a list at its item starts, code at its lines; then lines, sentences and word starts. Pieces
  leave room for the overlap tail.
- **Every chunk body is a verbatim slice of the cleaned page** (`char_start/char_end`), except inside a frame:
  a split code block gives slices of its inner lines inside the block's fences; a table too big for one chunk
  gives **one chunk per row under its header row** (a long row's later pieces also carry its first cell). Rows
  never share a chunk. Each chunk carries its frame, and `body == frame_head + slice + frame_tail` is checked
  exactly (`chunk.rebuilds`; the frames are not stored in silver).
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
  1,027 prose joins overlap, 18 meet at a paragraph break and 1 inside a paragraph without overlap; the other
  2,675 are at code blocks or table rows, where no overlap is taken by design.

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
  smaller chunks and each carries a longer breadcrumb. What it buys is locality: an edit never changes a chunk
  in another section, while the baseline lost 8 and 12 such chunks.
- Its largest single cost is tables: 338 lost IDs at 254/32 (340 at 128/16) are rows whose own text is
  unchanged but whose header row changed, so every row of the table re-embeds.
- v4.0.0 → v4.0.1 (a patch release, 4 modified docs) is almost the same for both: 21 vs 19 new IDs at 254/32.
- Unchanged docs keep identical chunk IDs in both transitions (97/97 and 208/208).

So the case for sections is retrieval quality (coherent chunks with a precise breadcrumb), which M6 measures,
not ID stability or embedding cost. If M6 shows no gain, the whole-page baseline is the simpler choice.

## D-010 — Chunk identity and the silver contract

- **Date:** 2026-10-19 · **Status:** Accepted for the design. **The silver-job evidence is pending:** Databricks
  refused every job submit and SQL statement on 2026-10-19 from 05:27Z (`runs/m2_d0*`; see D-001 Quota)

**Decision.**
- `chunk_id = sha256(json[path, section_path, embed_text, occurrence])[:32]`: content-addressed, with nothing
  tag-, position- or config-specific. An unchanged section keeps its IDs in every tag; any change to the
  embedded text, breadcrumb included, gives a new ID. `config_id = sha256(cleaner, chunker, max, overlap,
  vocab)[:16]` keys silver rows only.
- `silver_chunks` is keyed `(config_id, source_repo, version_tag, chunk_id)`, with one immutable chunking per
  `(config_id, tag)`, the bronze pattern: identical → MERGE skipped; different → the run fails, and names
  bronze moving under the stored rows before a missing version bump. Every run recomputes every stored row's
  chunk_id, embed text, n_tokens and versions, and **re-chunks every other stored tag of the config from bronze,
  which must reproduce exactly**, before writing. Off-grid configs are refused. A doc without chunks fails the
  run unless it is in `Config.zero_chunk_docs`.
- **Version discipline.** Golden files are never overwritten: fixture text, fixture chunks, and the real corpus
  (`tests/golden/corpus_c<cleaner>_k<chunker>.json`, checked in CI and by `run_job.py` before every silver job).
  Changed output needs a version bump, which gives a new config_id and new rows; a new grid config or tag only
  adds entries. Version 1's goldens were recreated during M2 before anything was published to silver.

**Why the corpus golden.** A cleaner or chunker change that passes every unit test can still change silver
rows (the last cleaner round did, D-008). Silver keys by config_id, so without the check a changed chunking
would be stored under the old version's key and mixed with rows from the old code.

**Consequences.**
- M3: the diff baseline is the last *published* (tag, commit) per config (D-006). Chunk-level change = added
  and removed chunk_ids; there is no chunk-level "changed".
- M5: gold and the index carry only chunk-determined columns (path, section_path, embed_text, n_tokens,
  offsets, vector, model); the gold MERGE is insert/delete only and asserts numUpdatedRows = 0; what to embed =
  chunk_ids not yet in gold.
- M6: label answers as (path, section_path, verbatim quote), resolve the quote to every occurrence in the
  cleaned text (the offsets name its coordinate system, `CLEANER_VERSION`), and count a chunk relevant if its
  slice overlaps any occurrence.

## Open questions (decide in the milestone noted)

- **M2/M5 — chunk size vs model context.** *Answered in M2 (D-009):* sizes are counted in the models' own
  WordPiece units, and the parity run shows no chunk of either config over either model's limit. M4 keeps the
  check as a DQ rule: a chunk is oversized when `n_tokens + 2` exceeds the embedding model's `max_seq_length`.
- **M3 — chunk ID stability.** *Answered in M2 (D-009, D-010):* content-addressed IDs within heading sections
  are local (an edit never changes a chunk in another section), but they do not cut re-embedding cost against
  a whole-page baseline. M3 counts chunk-level change as added and removed chunk_ids.
- **M4 — row-count drift threshold.** v3.5.0 → v4.0.0 legitimately removes 32 net docs (−13%). A
  hard-fail drift check needs a threshold plus an explicit, logged override. Otherwise it fires on
  every major release and someone disables it.
- **M6 — relevance labels must not depend on chunking.** Gold answers are labelled as
  (doc path, section) plus an answer span, given as a verbatim quote of the cleaned text (D-010). A retrieved
  chunk counts as relevant if its slice overlaps an occurrence of the quote. Otherwise chunk sizes cannot be compared. With n=50, the 95% CI half-width on recall@5 is
  ±8.3 / ±11.1 / ±13.9 points at recall 0.9 / 0.8 / 0.5 (normal approximation, 1.96·√(p(1−p)/50)),
  so we report bootstrap CIs and paired differences, not just point estimates.
