"""Assemble the eval set from the reviewed drafts (M6, D-015): the first --n usable items in sample order.

    uv run python scripts/m6_build_set.py --drafts eval/drafts_v1.json --version 1 [--n 50]

eval/drafts_v1.json holds every drafted item with the reviewer's verdict (keep, fixed, drop) and notes, one
per sampled section (eval/sections_<tag>.json). An item is usable when the reviewer kept or fixed it, it is
not a skip, and its quote is a verbatim substring of its section's body. Items are taken in the sample's order
(the seeded shuffle of scripts/m6_sample_sections.py) up to --n, so which sections the set covers was decided
before any retrieval was run; the rest of the usable items are spares, used in the same order when the owner
rejects an item. Writes eval/eval_set_v<version>.json (refuses to overwrite) and prints what it took and
left. The full checks (sections, occurrences, a relevant chunk under every config, silver's chunks) are
scripts/m6_eval.py --check-only.
"""

import argparse
import json
from typing import Any

from runguard import REPO


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drafts", required=True)
    ap.add_argument("--version", required=True)
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--reject", nargs="*", default=[], help="section ids the owner rejected (spares fill in)")
    args = ap.parse_args()
    out_path = REPO / "eval" / f"eval_set_v{args.version}.json"
    if out_path.exists():
        raise SystemExit(f"{out_path.relative_to(REPO)} exists: a set version is never overwritten")
    drafts = json.loads((REPO / args.drafts).read_text())
    sections_file = json.loads((REPO / drafts["sections_file"]).read_text())
    sections = {s["section_id"]: s for s in sections_file["sections"]}
    order = [s["section_id"] for s in sections_file["sections"]]
    by_section = {d["section_id"]: d for d in drafts["items"]}
    usable: list[dict[str, Any]] = []
    left: dict[str, str] = {}
    for sid in order:
        d = by_section.get(sid)
        if d is None:
            left[sid] = "no draft"
        elif d["verdict"] == "drop" or d["skip"]:
            left[sid] = f"{d['verdict']}{' (skip)' if d['skip'] else ''}: {d['review_note']}"
        elif d["quote"] not in sections[sid]["body"]:
            left[sid] = "quote is not a substring of its section body"
        elif sid in args.reject:
            left[sid] = "rejected by the owner"
        else:
            usable.append(d)
    if len(usable) < args.n:
        raise SystemExit(f"only {len(usable)} usable items for {args.n}")
    chosen = usable[: args.n]
    items = [
        {
            "item_id": f"q{i + 1:02d}",
            "section_id": d["section_id"],
            "question": d["question"],
            "answer": d["answer"],
            "path": sections[d["section_id"]]["path"],
            "section_path": sections[d["section_id"]]["section_path"],
            "quote": d["quote"],
            "kind": d["kind"],
            "also_in": d["also_in"],
            "verdict": d["verdict"],
            "review_note": d["review_note"],
        }
        for i, d in enumerate(chosen)
    ]
    out = {
        "version": args.version,
        "tag": sections_file["tag"],
        "commit_sha": sections_file["commit_sha"],
        "cleaner_version": sections_file["cleaner_version"],
        "drafts": args.drafts,
        "owner_rejected_sections": args.reject,
        "spares": [d["section_id"] for d in usable[args.n :]],
        "items": items,
    }
    out_path.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    print(f"{len(items)} items, {len(out['spares'])} spares; left out: {len(left)}")
    for sid, why in left.items():
        print(" ", sid, why[:150])


if __name__ == "__main__":
    main()
