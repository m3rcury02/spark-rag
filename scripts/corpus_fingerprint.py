"""The real corpus as a golden test: per (chunk config, tag), a fingerprint of every chunk-determined column.

    uv run python scripts/corpus_fingerprint.py            # check (CI, and run_job.py before a silver job)
    uv run python scripts/corpus_fingerprint.py --update   # add entries for new versions, configs or tags

The unit-test fixtures cannot hold every construct of the real pages, so a cleaner or chunker change can pass
pytest and still change silver rows. This script reads the tags from git (scripts/gitdocs.py, so it needs
network but no Databricks), chunks them with every config of Config.chunk_grid, and compares the fingerprints
with tests/golden/corpus_c<CLEANER_VERSION>_k<CHUNKER_VERSION>.json. A changed entry means: bump a version.
Like the other goldens, an existing entry is never overwritten; --update only adds the (config, tag) entries
the file lacks, so a config added to the grid, or a new tag, does not need a version bump.
"""

import argparse
import hashlib
import json
import sys
import time
from typing import Any

from gitdocs import read_tags
from runguard import REPO

from docs_rag.chunk import CHUNKER_VERSION, ChunkConfig, chunk_document
from docs_rag.clean import CLEANER_VERSION
from docs_rag.config import Config

GOLDEN = REPO / "tests" / "golden" / f"corpus_c{CLEANER_VERSION}_k{CHUNKER_VERSION}.json"
TAGS = ("v3.5.0", "v4.0.0", "v4.0.1")


def fingerprints(tags: tuple[str, ...], grid: tuple[ChunkConfig, ...]) -> dict[str, Any]:
    source = read_tags(list(tags))
    out: dict[str, Any] = {}
    for config in grid:
        per_tag = {}
        for tag in tags:
            lines = []
            for path, content in sorted(source[tag].files.items()):
                doc = chunk_document(path, content, config)
                for c in doc.chunks:
                    row = [path, c.chunk_id, c.chunk_index, c.occurrence, list(c.section_path), c.body]
                    row += [c.embed_text, c.n_tokens, c.char_start, c.char_end, doc.clean_sha256]
                    lines.append(json.dumps(row, ensure_ascii=False))
            digest = hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest()
            per_tag[tag] = {
                "source_commit": source[tag].commit_sha,
                "chunks": len(lines),
                "fingerprint": digest,
            }
        out[config.config_id] = {
            "max_tokens": config.max_tokens,
            "overlap_tokens": config.overlap_tokens,
            **per_tag,
        }
    return out


def compare(golden: dict[str, Any], current: dict[str, Any]) -> tuple[list[str], list[str]]:
    """(missing, changed): (config, tag) entries the golden lacks, and entries that differ from it."""
    missing, changed = [], []
    for config_id, entry in current.items():
        for tag, now in entry.items():
            if not isinstance(now, dict):
                continue
            was = golden.get(config_id, {}).get(tag)
            if was is None:
                missing.append(f"{config_id} ({entry['max_tokens']}/{entry['overlap_tokens']}) {tag}")
            elif was != now:
                changed.append(f"{config_id} {tag}: committed {was}, now {now}")
    return missing, changed


def merged(golden: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    """The golden with the current entries it lacks added; existing entries are kept as they are."""
    out = {config_id: dict(entry) for config_id, entry in golden.items()}
    for config_id, entry in current.items():
        out.setdefault(config_id, {}).update({k: v for k, v in entry.items() if k not in out[config_id]})
    return out


def check() -> list[str]:
    """Why the committed corpus golden and the current code disagree ([] when they agree)."""
    if not GOLDEN.exists():
        return [f"no {GOLDEN.name}: new versions need `scripts/corpus_fingerprint.py --update`"]
    missing, changed = compare(
        json.loads(GOLDEN.read_text(encoding="utf-8")), fingerprints(TAGS, Config().chunk_grid)
    )
    return [f"changed (bump CLEANER_VERSION or CHUNKER_VERSION): {c}" for c in changed] + [
        f"not in {GOLDEN.name} (a new grid config or tag: run --update to add it): {m}" for m in missing
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--update", action="store_true", help="add the entries the golden lacks (never changes one)"
    )
    args = ap.parse_args()
    t0 = time.monotonic()
    if args.update:
        golden = json.loads(GOLDEN.read_text(encoding="utf-8")) if GOLDEN.exists() else {}
        current = fingerprints(TAGS, Config().chunk_grid)
        missing, changed = compare(golden, current)
        if changed:
            sys.exit(
                f"{GOLDEN.name} is never overwritten; output changed, so bump a version:\n"
                + "\n".join(changed)
            )
        GOLDEN.write_text(json.dumps(merged(golden, current), indent=1) + "\n", encoding="utf-8")
        print(f"added {len(missing)} entries to {GOLDEN.relative_to(REPO)} in {time.monotonic() - t0:.0f} s")
        return
    problems = check()
    if problems:
        sys.exit("corpus output differs from the committed golden:\n" + "\n".join(problems))
    print(f"corpus fingerprints match {GOLDEN.name} ({time.monotonic() - t0:.0f} s)")


if __name__ == "__main__":
    main()
