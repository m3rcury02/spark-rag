import math

import pytest

from docs_rag.config import Config, EmbeddingConfig
from docs_rag.embedder import canary_cosines, cosine, load_canary


def test_the_shipped_canary_is_of_the_published_model() -> None:
    canary = load_canary()
    config = Config().embedding
    assert canary.model_id == config.model_id
    assert len(canary.texts) == len(canary.vectors) >= 3
    for v in canary.vectors:
        assert len(v) == config.dimension
        assert math.isclose(math.sqrt(sum(x * x for x in v)), 1.0, abs_tol=1e-4)
    assert canary_cosines(canary, config, canary.vectors) == pytest.approx([1.0] * len(canary.vectors))


def test_another_models_canary_is_refused() -> None:
    other = EmbeddingConfig(
        model="BAAI/bge-small-en-v1.5", revision="5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
    )
    with pytest.raises(RuntimeError, match="the canary is of"):
        canary_cosines(load_canary(), other, load_canary().vectors)


def test_cosine() -> None:
    assert cosine([1.0, 0.0], [0.0, 2.0]) == 0.0
    assert cosine([3.0, 4.0], [6.0, 8.0]) == pytest.approx(1.0)
