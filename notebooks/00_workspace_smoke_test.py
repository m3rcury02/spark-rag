# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Workspace smoke test (Databricks Free Edition)
# MAGIC
# MAGIC Answers the questions that decide D-001 (vector index platform). The docs cannot answer them for *your* workspace:
# MAGIC
# MAGIC 1. Which hosts can serverless compute reach (PyPI, GitHub, Hugging Face, PyTorch)?
# MAGIC 2. Can we install and run `sentence-transformers` on serverless CPU, and how fast is it?
# MAGIC 3. Can we create (or reuse) the single AI Search endpoint Free Edition allows?
# MAGIC 4. Does a **Delta Sync index with self-managed embeddings** sync, and do a source-table **UPDATE and DELETE propagate** after a triggered sync?
# MAGIC
# MAGIC **Cost:** Free Edition has no charges. Usage is capped by a fair-use quota. The notebook creates one AI Search endpoint if none exists. **Keep that endpoint**: the project reuses it, and Free Edition allows only one. (On a *paid* workspace an endpoint bills per hour while it exists.)
# MAGIC
# MAGIC **How to run:** Workspace → Import → this file. Attach to *Serverless* and **Run all**. Endpoint provisioning can take 10–30 min. Paste the JSON printed by the last cell back to Claude.

# COMMAND ----------

# Record the install start time in /tmp, because restartPython() clears Python state.
import importlib.util
import json
import time

pre = {"torch_preinstalled": importlib.util.find_spec("torch") is not None, "install_started": time.time()}
with open("/tmp/docs_rag_smoke_pre.json", "w") as f:
    json.dump(pre, f)
print(pre)

# COMMAND ----------

# MAGIC %pip install --quiet databricks-ai-search==0.78 sentence-transformers==6.1.0

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821  (dbutils is injected by Databricks)

# COMMAND ----------

import json
import os
import platform
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import timedelta

dbutils.widgets.text("catalog", "workspace")  # noqa: F821
dbutils.widgets.text("schema", "docs_rag_smoke")  # noqa: F821
dbutils.widgets.text("endpoint", "docs_rag_endpoint")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
ENDPOINT = dbutils.widgets.get("endpoint")  # noqa: F821

REPORT: dict = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
try:
    with open("/tmp/docs_rag_smoke_pre.json") as f:
        REPORT["install"] = json.load(f)
    REPORT["install"]["install_plus_restart_seconds"] = round(
        time.time() - REPORT["install"].pop("install_started"), 1
    )
except OSError as e:
    REPORT["install"] = {"error": f"pre-install record missing: {e}"}


def mem_total_gb() -> float | None:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        return None
    return None


REPORT["compute"] = {
    "python": sys.version.split()[0],
    "platform": platform.platform(),
    "cpu_count": os.cpu_count(),
    "mem_total_gb": mem_total_gb(),
    "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
    "spark_version": spark.version,  # noqa: F821
}
print(json.dumps(REPORT, indent=2))

# COMMAND ----------

# MAGIC %md ## 1. Outbound network
# MAGIC Any HTTP status code (even 403/404/429) means the host is **reachable**. A DNS or connection error means it is blocked.

# COMMAND ----------

PROBES = {
    "pypi.org": "https://pypi.org/simple/pip/",
    "files.pythonhosted.org": "https://files.pythonhosted.org/",
    "github.com": "https://github.com/apache/spark",
    "api.github.com": "https://api.github.com/repos/apache/spark/git/ref/tags/v4.0.1",
    "codeload.github.com": "https://codeload.github.com/apache/spark/tar.gz/refs/tags/v4.0.1",
    "raw.githubusercontent.com": "https://raw.githubusercontent.com/apache/spark/v4.0.1/docs/index.md",
    "huggingface.co": "https://huggingface.co/api/models/sentence-transformers/all-MiniLM-L6-v2",
    "download.pytorch.org": "https://download.pytorch.org/whl/cpu/",
}


def probe(host: str, url: str) -> dict:
    out: dict = {}
    try:
        socket.getaddrinfo(host, 443)
        out["dns"] = "ok"
    except OSError as e:
        return {"dns": f"FAIL {type(e).__name__}: {e}", "reachable": False}
    req = urllib.request.Request(url, method="HEAD" if "codeload" in host else "GET")
    req.add_header("User-Agent", "docs-rag-smoke-test")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            out["http_status"] = r.status
    except urllib.error.HTTPError as e:
        out["http_status"] = e.code
    except Exception as e:  # noqa: BLE001 — we want every failure mode recorded
        out["error"] = f"{type(e).__name__}: {e}"[:300]
    out["reachable"] = "http_status" in out
    out["seconds"] = round(time.time() - t0, 2)
    return out


REPORT["network"] = {h: probe(h, u) for h, u in PROBES.items()}
for h, r in REPORT["network"].items():
    print(f"{h:28s} reachable={r['reachable']!s:5s} {r.get('http_status', r.get('error', r.get('dns')))}")

# COMMAND ----------

# MAGIC %md ## 2. Embedding models on serverless CPU
# MAGIC Texts are ~970 characters (189 word-piece tokens for both models), close to a real chunk, so the throughput is realistic.

# COMMAND ----------

PARAGRAPH = (
    "Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL "
    "engine. You can express your streaming computation the same way you would express a batch computation "
    "on static data. The Spark SQL engine will take care of running it incrementally and continuously and "
    "updating the final result as streaming data continues to arrive. The system ensures end-to-end "
    "exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead Logs. "
)
TEXTS = [f"[{i}] " + PARAGRAPH * 2 for i in range(256)]
MODELS = ["sentence-transformers/all-MiniLM-L6-v2", "BAAI/bge-small-en-v1.5"]
REPORT["embeddings"] = {}
EMBEDDER = None  # first model that loads; reused by the index test

try:
    import torch
    from sentence_transformers import SentenceTransformer

    REPORT["install"]["torch_version"] = torch.__version__
    REPORT["install"]["torch_threads"] = torch.get_num_threads()
    for name in MODELS:
        r: dict = {}
        try:
            t0 = time.time()
            m = SentenceTransformer(name, device="cpu")
            r["load_seconds"] = round(time.time() - t0, 1)
            r["dim"] = m.get_embedding_dimension()
            r["max_seq_length"] = m.max_seq_length
            r["text_token_len_sample"] = len(m.tokenizer(TEXTS[0])["input_ids"])
            t0 = time.time()
            m.encode(TEXTS, batch_size=32, normalize_embeddings=True, show_progress_bar=False)
            dt = time.time() - t0
            r["encode_seconds_256_texts"] = round(dt, 1)
            r["texts_per_second"] = round(len(TEXTS) / dt, 1)
            if EMBEDDER is None:
                EMBEDDER = m
        except Exception as e:  # noqa: BLE001
            r["error"] = f"{type(e).__name__}: {e}"[:500]
        REPORT["embeddings"][name] = r
except Exception as e:  # noqa: BLE001
    REPORT["embeddings"]["import_error"] = f"{type(e).__name__}: {e}"[:500]
print(json.dumps(REPORT["embeddings"], indent=2))

# COMMAND ----------

# MAGIC %md ## 3. AI Search endpoint (Free Edition allows one)

# COMMAND ----------

from databricks.ai_search.client import AISearchClient

client = AISearchClient(disable_notice=True)
EP_NAME = None
try:
    existing = client.list_endpoints().get("endpoints", [])
    REPORT["endpoint"] = {"existing": [{"name": e["name"], "type": e.get("endpoint_type")} for e in existing]}
    if existing:
        names = [e["name"] for e in existing]
        EP_NAME = ENDPOINT if ENDPOINT in names else names[0]
        REPORT["endpoint"]["action"] = f"reused {EP_NAME}"
    else:
        client.create_endpoint(name=ENDPOINT, endpoint_type="STANDARD")
        EP_NAME = ENDPOINT
        REPORT["endpoint"]["action"] = f"created {EP_NAME} (STANDARD)"
    t0 = time.time()
    client.wait_for_endpoint(EP_NAME, verbose=True, timeout=timedelta(minutes=45))
    REPORT["endpoint"]["wait_online_seconds"] = round(time.time() - t0, 1)
    ep = client.get_endpoint(EP_NAME)
    REPORT["endpoint"]["type"] = ep.get("endpoint_type")
    REPORT["endpoint"]["state"] = ep.get("endpoint_status", {}).get("state")
except Exception as e:  # noqa: BLE001
    REPORT.setdefault("endpoint", {})["error"] = f"{type(e).__name__}: {e}"[:800]
print(json.dumps(REPORT["endpoint"], indent=2))

# COMMAND ----------

# MAGIC %md ## 4. Delta Sync index (self-managed embeddings): create → query → UPDATE + DELETE → sync → verify

# COMMAND ----------

SRC = f"{CATALOG}.{SCHEMA}.smoke_src"
IDX = f"{CATALOG}.{SCHEMA}.smoke_idx"
ROWS = [
    ("c1", "Spark SQL can run in ANSI mode for stricter type checking."),
    ("c2", "Structured Streaming stores progress in a checkpoint location."),
    ("c3", "This row will be deleted to test delete propagation."),
    ("c4", "This row will be updated to test update propagation."),
    ("c5", "Spark can run on Kubernetes using the native scheduler."),
]
UPDATED_C4 = "Adaptive Query Execution re-optimizes plans at runtime using shuffle statistics."
idx_report: dict = {}
REPORT["delta_sync"] = idx_report


def embed(texts: list[str]) -> list[list[float]]:
    if EMBEDDER is not None:
        return [[float(x) for x in v] for v in EMBEDDER.encode(texts, normalize_embeddings=True)]
    raise RuntimeError("no embedding model loaded; see section 2")


def ids_in_index(index, dim: int) -> dict[str, str]:
    """Return {id: text} for every row in this tiny index (num_results > row count)."""
    res = index.similarity_search(query_vector=[1.0 / dim**0.5] * dim, columns=["id", "text"], num_results=50)
    return {row[0]: row[1] for row in res["result"].get("data_array", [])}


try:
    vecs = embed([t for _, t in ROWS])
    dim = len(vecs[0])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
    try:  # remove an index left over from a previous run of this notebook
        client.delete_index(endpoint_name=EP_NAME, index_name=IDX)
        time.sleep(30)
    except Exception:  # noqa: BLE001
        pass
    spark.sql(  # noqa: F821
        f"CREATE OR REPLACE TABLE {SRC} (id STRING NOT NULL, text STRING, embedding ARRAY<FLOAT>) "
        "TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
    spark.createDataFrame(  # noqa: F821
        [(i, t, v) for (i, t), v in zip(ROWS, vecs, strict=True)],
        "id STRING, text STRING, embedding ARRAY<FLOAT>",
    ).write.mode("append").saveAsTable(SRC)

    t0 = time.time()
    index = client.create_delta_sync_index(
        endpoint_name=EP_NAME,
        index_name=IDX,
        source_table_name=SRC,
        primary_key="id",
        pipeline_type="TRIGGERED",
        embedding_dimension=dim,
        embedding_vector_column="embedding",
        columns_to_sync=["id", "text"],
    )
    index.wait_until_ready(verbose=True, timeout=timedelta(minutes=45))
    idx_report["create_to_ready_seconds"] = round(time.time() - t0, 1)
    desc = index.describe()
    idx_report["detailed_state"] = desc.get("status", {}).get("detailed_state")
    idx_report["indexed_row_count_initial"] = desc.get("status", {}).get("indexed_row_count")

    q = embed(["Where does streaming keep its progress?"])[0]
    top = index.similarity_search(query_vector=q, columns=["id", "text"], num_results=1)["result"][
        "data_array"
    ]
    idx_report["query_top1_id"] = top[0][0] if top else None
    idx_report["query_top1_expected"] = "c2"

    # Mutate the source: delete c3, update c4 (new text + new vector), then trigger a sync.
    spark.sql(f"DELETE FROM {SRC} WHERE id = 'c3'")  # noqa: F821
    spark.createDataFrame(  # noqa: F821
        [("c4", UPDATED_C4, embed([UPDATED_C4])[0])], "id STRING, text STRING, embedding ARRAY<FLOAT>"
    ).createOrReplaceTempView("smoke_upd")
    spark.sql(f"MERGE INTO {SRC} t USING smoke_upd s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *")  # noqa: F821

    t0 = time.time()
    index.sync()
    deadline = time.time() + 30 * 60
    rows_now: dict[str, str] = {}
    while time.time() < deadline:  # poll instead of trusting state names: avoids a race right after sync()
        time.sleep(20)
        rows_now = ids_in_index(index, dim)
        if "c3" not in rows_now and rows_now.get("c4") == UPDATED_C4:
            break
    idx_report["sync_to_visible_seconds"] = round(time.time() - t0, 1)
    idx_report["ids_after_sync"] = sorted(rows_now)
    idx_report["delete_propagated"] = "c3" not in rows_now
    idx_report["update_propagated"] = rows_now.get("c4") == UPDATED_C4
    idx_report["indexed_row_count_after"] = index.describe().get("status", {}).get("indexed_row_count")
except Exception as e:  # noqa: BLE001
    idx_report["error"] = f"{type(e).__name__}: {e}"[:1200]
print(json.dumps(idx_report, indent=2))

# COMMAND ----------

# MAGIC %md ## 5. Summary — copy this JSON back to Claude

# COMMAND ----------

net = REPORT.get("network", {})
emb = {k: v for k, v in REPORT.get("embeddings", {}).items() if isinstance(v, dict)}
REPORT["verdict"] = {
    "packages_importable": "import_error" not in REPORT.get("embeddings", {}),
    "github_reachable": any(
        net.get(h, {}).get("reachable")
        for h in ("api.github.com", "raw.githubusercontent.com", "codeload.github.com")
    ),
    "huggingface_model_loaded": any("error" not in v for v in emb.values()) if emb else False,
    "endpoint_online": REPORT.get("endpoint", {}).get("state") == "ONLINE",
    "delta_sync_delete_propagates": idx_report.get("delete_propagated"),
    "delta_sync_update_propagates": idx_report.get("update_propagated"),
}
REPORT["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
print(json.dumps(REPORT, indent=2, default=str))
# A submitted (Jobs API) run returns this value from `get-run-output`; printed output is not returned.
dbutils.notebook.exit(json.dumps(REPORT, default=str))  # noqa: F821

# COMMAND ----------

# MAGIC %md ## 6. Cleanup (optional): drops the test index, table and schema. **Keeps the endpoint.**

# COMMAND ----------

# Uncomment to clean up after you have copied the JSON above.
# client.delete_index(endpoint_name=EP_NAME, index_name=IDX)
# spark.sql(f"DROP TABLE IF EXISTS {SRC}")
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA}")
