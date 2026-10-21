"""Draw the sections the M6 eval questions are written from: a seeded sample, one section per doc (D-015).

    uv run --with databricks-sdk==0.152.0 python scripts/m6_sample_sections.py --label m6_a_sections

Reads the tag's docs from git and cleans them with today's cleaner (the text the chunk offsets point into).
A section is eligible when its body holds at least MIN_PROSE characters outside code blocks and table rows
(a question needs prose to stand on) and the doc is not a stub or a doc allowed to have no chunks. The docs
with an eligible section are shuffled with the seed and the first --n are kept, one eligible section of each
drawn with the same generator: one per doc, so the long pages (configuration, SQL reference) do not crowd out
the rest. Writes eval/sections_<tag>.json (the sections' text, for drafting questions) and runs/<label>.json
(seed, criteria, counts, and the sample with each body's sha256). Git only; Databricks just for redaction.
"""

import argparse
import hashlib
import json
import random
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import read_tags
from redact import Redactor
from runguard import REPO, committed_head

from docs_rag.clean import CLEANER_VERSION, clean_page, fenced_blocks
from docs_rag.config import Config, validate_tag
from docs_rag.sections import split_sections

MIN_PROSE = 300
STUB = "This document has moved"


def prose_chars(body: str) -> int:
    lines = body.split("\n")
    code = {i for first, last in fenced_blocks(lines) for i in range(first, last + 1)}
    return sum(
        len(line) for i, line in enumerate(lines) if i not in code and not line.lstrip().startswith("|")
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tag", default="v4.0.1")
    ap.add_argument("--n", type=int, default=75)
    ap.add_argument("--seed", type=int, default=6)
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    tag = validate_tag(args.tag)
    sections_path = REPO / "eval" / f"sections_{tag}.json"
    for p in (out_path, sections_path):
        if p.exists():
            raise SystemExit(f"{p.relative_to(REPO)} exists: never overwritten")
    head = committed_head()
    redactor = Redactor(WorkspaceClient())
    config = Config()
    source = read_tags([tag])[tag]

    eligible: dict[str, list[dict[str, Any]]] = {}
    sections_seen = 0
    for path, raw in sorted(source.files.items()):
        if path in config.zero_chunk_docs:
            continue
        page = clean_page(raw, fallback_title=path.rsplit("/", 1)[-1].removesuffix(".md"))
        if STUB in page.text:
            continue
        for s in split_sections(page.title, page.text):
            sections_seen += 1
            if prose_chars(s.body) >= MIN_PROSE:
                eligible.setdefault(path, []).append(
                    {
                        "section_path": list(s.path),
                        "start": s.start,
                        "end": s.start + len(s.body),
                        "body": s.body,
                    }
                )
    rng = random.Random(args.seed)
    docs = sorted(eligible)
    rng.shuffle(docs)
    sample = []
    for i, path in enumerate(docs[: args.n]):
        s = rng.choice(eligible[path])
        sample.append(
            {
                "section_id": f"s{i + 1:03d}",
                "path": path,
                **s,
                "url": f"https://github.com/apache/spark/blob/{source.commit_sha}/{path}",
            }
        )
    sections_path.parent.mkdir(exist_ok=True)
    sections_path.write_text(
        json.dumps(
            {
                "tag": tag,
                "commit_sha": source.commit_sha,
                "cleaner_version": CLEANER_VERSION,
                "sections": sample,
            },
            indent=1,
            ensure_ascii=False,
        )
        + "\n"
    )
    record = {
        "label": args.label,
        "git_commit": head,
        "tag": tag,
        "commit_sha": source.commit_sha,
        "cleaner_version": CLEANER_VERSION,
        "seed": args.seed,
        "n": args.n,
        "criteria": {
            "min_prose_chars": MIN_PROSE,
            "excluded": "zero-chunk docs, stub pages, code and table lines",
            "draw": "one section per doc",
        },
        "docs": len(source.files),
        "sections_seen": sections_seen,
        "docs_with_an_eligible_section": len(eligible),
        "eligible_sections": sum(len(v) for v in eligible.values()),
        "sample": [
            {
                "section_id": s["section_id"],
                "path": s["path"],
                "section_path": s["section_path"],
                "body_chars": len(s["body"]),
                "body_sha256": hashlib.sha256(s["body"].encode()).hexdigest(),
            }
            for s in sample
        ],
        "sections_file": str(sections_path.relative_to(REPO)),
    }
    out_path.write_text(redactor.dump(record))
    print(json.dumps({k: v for k, v in record.items() if k != "sample"}, indent=2))


if __name__ == "__main__":
    main()
