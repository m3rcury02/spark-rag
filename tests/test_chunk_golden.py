"""Golden chunks: any change to chunk text, token counts, IDs or offsets for the same input must bump
CHUNKER_VERSION (or CLEANER_VERSION, when tests/test_clean_golden.py fails too).

Silver rows are keyed by config_id, which includes both versions, so a change that ships without a bump would
mix two chunkings under one key. This also catches a tokenizer or vocab change. To regenerate after a
deliberate change: bump the version, then run
`UPDATE_GOLDEN=1 uv run pytest tests/test_clean_golden.py tests/test_chunk_golden.py`. An existing golden file
is never overwritten. The fixtures cannot hold every real page; scripts/corpus_fingerprint.py checks the
real corpus the same way (in CI, and before every silver job).
"""

from golden_inputs import HERE, check_golden, page

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig, chunk_document
from docs_rag.clean import CLEANER_VERSION

GOLDEN = HERE / "golden" / f"chunks_c{CLEANER_VERSION}_k{CHUNKER_VERSION}.json"
CONFIGS = [
    ChunkConfig(max_tokens=64, overlap_tokens=8),
    ChunkConfig(max_tokens=128, overlap_tokens=16),
    ChunkConfig(),
]


def current() -> dict[str, list[dict[str, object]]]:
    return {
        config.config_id: [
            {
                "chunk_id": c.chunk_id,
                "n_tokens": c.n_tokens,
                "span": [c.char_start, c.char_end],
                "embed_text": c.embed_text,
            }
            for c in chunk_document("docs/quick-start.md", page(), config).chunks
        ]
        for config in CONFIGS
    }


def test_chunker_output_matches_the_golden_file_for_this_version() -> None:
    check_golden(GOLDEN, current(), "CHUNKER_VERSION (or CLEANER_VERSION if the cleaned text changed)")
