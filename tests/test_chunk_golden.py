"""Golden output: any change to chunk text, token counts or IDs for the same input must bump CHUNKER_VERSION.

Silver rows are keyed by config_id, which includes CHUNKER_VERSION, so a chunker change that ships without
a bump would mix two chunkings under one key. This also catches a tokenizer or vocab change. To regenerate
after a deliberate change: bump CHUNKER_VERSION, then run
`UPDATE_GOLDEN=1 uv run pytest tests/test_chunk_golden.py`.
"""

import json
import os
from pathlib import Path

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig, chunk_document

HERE = Path(__file__).parent
GOLDEN = HERE / "golden" / f"chunks_v{CHUNKER_VERSION}.json"

# The fixture page, plus sections that force each split path: long paragraph, long code block, long word.
EXTRA = (
    "\n\n## Long paragraph\n\n"
    + " ".join(
        f"Setting spark.sql.shuffle.partitions to {n} changes how many tasks a shuffle stage runs."
        for n in range(30)
    )
    + "\n\n## Long code\n\n```python\n"
    + "\n".join(f"df{n} = spark.range({n}).withColumn('x', col('id') * {n})" for n in range(30))
    + "\n```\n\n## Long word\n\n"
    + ("bcdfghjklmnpqrstvwxz" * 5)[:99]  # one word, more tokens than the small config's budget
    + "\n"
)
CONFIGS = [ChunkConfig(max_tokens=64, overlap_tokens=8), ChunkConfig()]


def current() -> dict[str, list[dict[str, object]]]:
    page = (HERE / "fixtures" / "spark_page.md").read_text() + EXTRA
    return {
        config.config_id: [
            {"chunk_id": c.chunk_id, "n_tokens": c.n_tokens, "embed_text": c.embed_text}
            for c in chunk_document("docs/quick-start.md", page, config).chunks
        ]
        for config in CONFIGS
    }


def test_chunker_output_matches_the_golden_file_for_this_version() -> None:
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(json.dumps(current(), indent=1, ensure_ascii=False) + "\n")
    assert GOLDEN.exists(), f"no golden file for CHUNKER_VERSION {CHUNKER_VERSION}; see the module docstring"
    expected = json.loads(GOLDEN.read_text())
    assert current() == expected, (
        "chunker output changed: bump CHUNKER_VERSION and regenerate the golden file"
    )
