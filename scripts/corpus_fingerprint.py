"""The real corpus as a golden test: per (chunk config, tag), a fingerprint of every chunk-determined column.

    uv run python scripts/corpus_fingerprint.py            # check (CI, and run_job.py before a silver job)
    uv run python scripts/corpus_fingerprint.py --update   # create the file for new versions

The unit-test fixtures cannot hold every construct of the real pages, so a cleaner or chunker change can pass
pytest and still change silver rows. This script reads the tags from git (scripts/gitdocs.py, so it needs
network but no Databricks), chunks them with every config of Config.chunk_grid, and compares the fingerprints
with tests/golden/corpus_c<CLEANER_VERSION>_k<CHUNKER_VERSION>.json. A difference means: bump a version. Like
the other goldens, an existing file is never overwritten.
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


def check() -> list[str]:
    """Differences between the committed corpus golden and the current code ([] when they agree)."""
    if not GOLDEN.exists():
        return [f"no {GOLDEN.name}: new versions need `scripts/corpus_fingerprint.py --update`"]
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    current = fingerprints(TAGS, Config().chunk_grid)
    problems = []
    for config_id, tags in current.items():
        for tag, now in tags.items():
            if not isinstance(now, dict):
                continue
            was = golden.get(config_id, {}).get(tag)
            if was != now:
                problems.append(f"{config_id} {tag}: committed {was}, now {now}")
    return problems


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true", help="create the golden file for the current versions")
    args = ap.parse_args()
    t0 = time.monotonic()
    if args.update:
        if GOLDEN.exists():
            sys.exit(f"{GOLDEN.name} exists and is never overwritten; bump a version if the output changed")
        GOLDEN.write_text(
            json.dumps(fingerprints(TAGS, Config().chunk_grid), indent=1) + "\n", encoding="utf-8"
        )
        print(f"wrote {GOLDEN.relative_to(REPO)} in {time.monotonic() - t0:.0f} s")
        return
    problems = check()
    if problems:
        sys.exit(
            "corpus output differs from the committed golden: bump CLEANER_VERSION or CHUNKER_VERSION\n"
            + "\n".join(problems)
        )
    print(f"corpus fingerprints match {GOLDEN.name} ({time.monotonic() - t0:.0f} s)")


if __name__ == "__main__":
    main()
