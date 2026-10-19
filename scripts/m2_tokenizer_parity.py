"""Check that our token counts are what the embedding models see, and that no chunk gets truncated.

    uv run --with databricks-sdk==0.152.0 --with sentence-transformers==6.1.0 --with transformers==5.19.0 \
        --with torch==2.14.1 --index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match \
        python scripts/m2_tokenizer_parity.py --label m2_tokenizer_parity --tags v3.5.0 v4.0.0 v4.0.1

Chunks every tag's docs (read from git) with every config of the chunk grid, then passes every distinct
embed text through each model's own `SentenceTransformer.preprocess`, the call `encode` makes. For every text
the number of attended positions must be n_tokens + 2 ([CLS] and [SEP]) and at most the model's
max_seq_length: a smaller number would mean the model truncates the chunk. Each model's vocab.txt must
be the vendored one (same sha256). Package versions and model revisions are recorded.
"""

import argparse
import hashlib
import importlib.metadata
import json
import pathlib
import platform
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from huggingface_hub import snapshot_download
from redact import Redactor
from runguard import REPO, committed_head
from sentence_transformers import SentenceTransformer

from docs_rag.chunk import chunk_document
from docs_rag.config import Config, validate_tag
from docs_rag.tokens import VOCAB_SHA256

MODELS = ("sentence-transformers/all-MiniLM-L6-v2", "BAAI/bge-small-en-v1.5")
BATCH = 256


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True)
    args = ap.parse_args()
    head = committed_head()
    tags = [validate_tag(t) for t in args.tags]
    grid = Config().chunk_grid
    source = read_tags(tags)
    counts: dict[str, int] = {}
    for config in grid:
        for tag in tags:
            for path, content in sorted(source[tag].files.items()):
                for c in chunk_document(path, content, config).chunks:
                    counts[c.embed_text] = c.n_tokens
    texts = sorted(counts)

    models: dict[str, Any] = {}
    for name in MODELS:
        model = SentenceTransformer(name, device="cpu")
        limit = model.max_seq_length
        seen: list[int] = []
        for i in range(0, len(texts), BATCH):
            features = model.preprocess(texts[i : i + BATCH])  # what encode() calls; documents get no prompt
            seen += [int(x) for x in features["attention_mask"].sum(dim=1)]
        mismatches = [(t, counts[t] + 2, s) for t, s in zip(texts, seen, strict=True) if s != counts[t] + 2]
        snapshot = pathlib.Path(snapshot_download(name, local_files_only=True))
        vocab = hashlib.sha256((snapshot / "vocab.txt").read_bytes()).hexdigest()
        models[name] = {
            "max_seq_length": limit,
            "revision": snapshot.name,
            "vocab_sha256": vocab,
            "vocab_is_the_vendored_one": vocab == VOCAB_SHA256,
            "texts": len(texts),
            "mismatches": len(mismatches),
            "mismatch_examples": [{"expected": e, "model": s, "text": t[:200]} for t, e, s in mismatches[:5]],
            "longest_input_positions": max(seen),
            "texts_at_max_seq_length": sum(s >= limit for s in seen),
            "texts_longer_than_the_model_accepts": sum(counts[t] + 2 > limit for t in texts),
            # encode() resolves its prompt from these; documents must get none
            "prompts": model.prompts,
            "default_prompt_name": model.default_prompt_name,
        }
    out = {
        "label": args.label,
        "git_commit": head,
        "chunk_configs": [{**c.__dict__, "config_id": c.config_id} for c in grid],
        "source_commits": {t: source[t].commit_sha for t in tags},
        "packages": {
            p: importlib.metadata.version(p)
            for p in ("sentence-transformers", "transformers", "tokenizers", "torch")
        },
        "machine": platform.machine(),
        "models": models,
        # A truncated text shows up as a mismatch (fewer positions than n_tokens + 2): the whole check.
        "all_pass": all(m["mismatches"] == 0 and m["vocab_is_the_vendored_one"] for m in models.values()),
    }
    (REPO / "runs" / f"{args.label}.json").write_text(Redactor(WorkspaceClient()).dump(out))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
