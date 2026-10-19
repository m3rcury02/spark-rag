"""The corpus golden's update rule (scripts/corpus_fingerprint.py): entries are added, never changed."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from corpus_fingerprint import compare, merged  # noqa: E402

A = {"source_commit": "c", "chunks": 3, "fingerprint": "f1"}
B = {"source_commit": "c", "chunks": 4, "fingerprint": "f2"}
GOLDEN = {"cfg1": {"max_tokens": 254, "overlap_tokens": 32, "v1": A}}


def test_a_new_tag_or_grid_config_is_missing_not_changed() -> None:
    current = {
        "cfg1": {"max_tokens": 254, "overlap_tokens": 32, "v1": A, "v2": B},
        "cfg2": {"max_tokens": 128, "overlap_tokens": 16, "v1": B},
    }
    missing, changed = compare(GOLDEN, current)
    assert (missing, changed) == (["cfg1 (254/32) v2", "cfg2 (128/16) v1"], [])
    assert merged(GOLDEN, current) == current
    assert compare(merged(GOLDEN, current), current) == ([], [])


def test_a_changed_entry_is_reported_and_never_merged_over() -> None:
    current = {"cfg1": {"max_tokens": 254, "overlap_tokens": 32, "v1": B}}
    missing, changed = compare(GOLDEN, current)
    assert missing == [] and len(changed) == 1 and changed[0].startswith("cfg1 v1")
    assert merged(GOLDEN, current) == GOLDEN
