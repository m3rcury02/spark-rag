"""Golden cleaned text: any change to cleaning output for the same input must bump CLEANER_VERSION.

Chunk offsets point into the cleaned text, so the cleaner version names their coordinate system (D-010). To
regenerate after a deliberate change: bump CLEANER_VERSION in src/docs_rag/clean.py, then run
`UPDATE_GOLDEN=1 uv run pytest tests/test_clean_golden.py tests/test_chunk_golden.py`.
"""

import json
import os

from golden_inputs import HERE, page

from docs_rag.clean import CLEANER_VERSION, clean_page

GOLDEN = HERE / "golden" / f"clean_v{CLEANER_VERSION}.json"


def current() -> dict[str, str]:
    doc = clean_page(page(), fallback_title="quick-start")
    return {"title": doc.title, "text": doc.text}


def test_cleaner_output_matches_the_golden_file_for_this_version() -> None:
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(json.dumps(current(), indent=1, ensure_ascii=False) + "\n")
    assert GOLDEN.exists(), f"no golden file for CLEANER_VERSION {CLEANER_VERSION}; see the module docstring"
    assert current() == json.loads(GOLDEN.read_text()), "cleaner output changed: bump CLEANER_VERSION"
