"""Golden cleaned text: any change to cleaning output for the same input must bump CLEANER_VERSION.

Chunk offsets point into the cleaned text, so the cleaner version names their coordinate system (D-010). To
regenerate after a deliberate change: bump CLEANER_VERSION in src/docs_rag/clean.py, then run
`UPDATE_GOLDEN=1 uv run pytest tests/test_clean_golden.py tests/test_chunk_golden.py`. An existing golden file
is never overwritten (golden_inputs.check_golden).
"""

from golden_inputs import HERE, check_golden, page

from docs_rag.clean import CLEANER_VERSION, clean_page

GOLDEN = HERE / "golden" / f"clean_v{CLEANER_VERSION}.json"


def test_cleaner_output_matches_the_golden_file_for_this_version() -> None:
    doc = clean_page(page(), fallback_title="quick-start")
    check_golden(GOLDEN, {"title": doc.title, "text": doc.text, "residue": doc.residue}, "CLEANER_VERSION")
