"""Make the embedding canary: fixed texts and the vectors the published model gives them (D-014).

    uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
        --with torch==2.14.1+cpu --index https://download.pytorch.org/whl/cpu \
        --index-strategy unsafe-best-match python scripts/m5_canary.py --label m5_a_canary

Encodes CANARY_TEXTS with docs_rag.embedder (the pinned model and revision of Config.embedding, normalized,
on CPU) and writes src/docs_rag/data/embedding_canary.json, which ships in the wheel. Every job that embeds
encodes the same texts and must reproduce these vectors (cosine >= Config.dq.min_canary_cosine) before
anything reaches gold: a wrong model, revision, pooling or normalization changes them while every other
vector check (dimension, finite, unit norm) still passes. The canary is made once per model; an existing
file is never overwritten (a model change is a new gold table, D-014).

Also writes runs/<label>.json: the package versions, the machine, the revision the cache resolved, and the
canary file's sha256. Databricks is used only to redact, as by every runs/ writer.
"""

import argparse
import hashlib
import importlib.metadata
import json
import pathlib
import platform

from databricks.sdk import WorkspaceClient
from huggingface_hub import snapshot_download
from redact import Redactor
from runguard import REPO, committed_head

from docs_rag.config import Config
from docs_rag.embedder import CANARY_FILE, encode, load_model

# Short and long, prose and code, the breadcrumb form of an embed text: what the corpus holds.
CANARY_TEXTS = (
    "Spark SQL is a Spark module for structured data processing.",
    "Structured Streaming Programming Guide > Handling Late Data and Watermarking\n\n"
    "A watermark tracks a point in event time before which we assume no more late data is going to arrive.",
    'df = spark.read.format("json").load("examples/src/main/resources/people.json")\ndf.show()',
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    canary_path = REPO / "src" / "docs_rag" / "data" / CANARY_FILE
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    if canary_path.exists():
        raise SystemExit(f"{canary_path.relative_to(REPO)} exists: a canary is never overwritten")
    head = committed_head()
    redactor = Redactor(WorkspaceClient())
    config = Config().embedding
    packages = {name: importlib.metadata.version(name) for name, _ in config.packages}
    wrong = {name: (packages[name], want) for name, want in config.packages if packages[name] != want}
    if wrong:
        raise SystemExit(f"installed packages differ from Config.embedding.packages: {wrong}")

    model = load_model(config)
    vectors = encode(model, CANARY_TEXTS, config)
    snapshot = pathlib.Path(snapshot_download(config.model, revision=config.revision, local_files_only=True))
    if snapshot.name != config.revision:
        raise SystemExit(f"the cache resolved revision {snapshot.name}, not {config.revision}")
    data = {"model_id": config.model_id, "texts": list(CANARY_TEXTS), "vectors": vectors}
    text = json.dumps(data, indent=1) + "\n"
    canary_path.write_text(text)
    record = {
        "label": args.label,
        "git_commit": head,
        "model_id": config.model_id,
        "resolved_revision": snapshot.name,
        "packages": packages,
        "machine": platform.machine(),
        "python": platform.python_version(),
        "canary_file": str(canary_path.relative_to(REPO)),
        "canary_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "texts": len(CANARY_TEXTS),
        "dimension": [len(v) for v in vectors],
        "norms": [round(sum(x * x for x in v) ** 0.5, 6) for v in vectors],
    }
    out_path.write_text(redactor.dump(record))
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
