"""Embedding with the published model (D-014), shared by the job, the canary script and the verifier, so all
three encode the same way: the pinned model revision, normalized vectors, the configured batch size.

sentence-transformers (and torch) are imported only when a model is loaded: the core and its unit tests do not
need them, and only the jobs and scripts that embed install them.
"""

import importlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from typing import Any

from docs_rag.config import EmbeddingConfig

CANARY_FILE = "embedding_canary.json"


def load_model(config: EmbeddingConfig) -> Any:
    st = importlib.import_module("sentence_transformers")
    return st.SentenceTransformer(config.model, revision=config.revision, device="cpu")


def encode(model: Any, texts: Sequence[str], config: EmbeddingConfig) -> list[list[float]]:
    vectors = model.encode(
        list(texts), batch_size=config.batch_size, normalize_embeddings=True, show_progress_bar=False
    )
    return [[float(x) for x in v] for v in vectors]


@dataclass(frozen=True)
class Canary:
    """Fixed texts and the vectors the published model gave them when the canary was made
    (scripts/m5_canary.py): a run whose model does not reproduce them has the wrong model or pooling."""

    model_id: str
    texts: tuple[str, ...]
    vectors: tuple[tuple[float, ...], ...]


def load_canary() -> Canary:
    data = json.loads(resources.files("docs_rag").joinpath("data", CANARY_FILE).read_text())
    return Canary(data["model_id"], tuple(data["texts"]), tuple(tuple(v) for v in data["vectors"]))


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def canary_cosines(
    canary: Canary, config: EmbeddingConfig, vectors: Sequence[Sequence[float]]
) -> list[float]:
    """Cosine of each canary text's vector now with its reference; refused for another model's canary."""
    if canary.model_id != config.model_id:
        raise RuntimeError(f"the canary is of {canary.model_id}, the published model is {config.model_id}")
    return [cosine(a, b) for a, b in zip(vectors, canary.vectors, strict=True)]
