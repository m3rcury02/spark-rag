"""Calibrate the DQ gate's thresholds on the real corpus: what each check measures across Spark's releases.

    uv run --with databricks-sdk==0.152.0 python scripts/m4_calibrate.py --label m4_a_calibration

Reads every final release tag from v3.0.0 on straight from git (scripts/gitdocs.py; the tag list from
`git ls-remote`), chunks each with Config.chunk, and runs docs_rag.dq.evaluate on it with the previous release
(in version order) as the baseline. Writes runs/<label>.json with:

- per tag: commit, docs, chunks, tokens, and every check's numerator, denominator and status;
- per transition (consecutive releases in version order, labelled major / minor / patch): the drift of docs,
  chunks and tokens, and which drift checks fail at Config.dq.max_drift;
- the firing rate of the drift rule at several thresholds, by transition kind;
- along D-002's path (v3.5.0 initial, v4.0.0, v4.0.1, v4.0.1 again), the full results with examples, for every
  config of Config.chunk_grid.

Git only: Databricks is used just to redact account identifiers, as every runs/ writer does.
"""

import argparse
import dataclasses
import datetime as dt
import re
import subprocess
from collections import Counter
from typing import Any

from databricks.sdk import WorkspaceClient
from gitdocs import SOURCE, TagDocs, read_tags
from redact import Redactor
from runguard import REPO, committed_head

from docs_rag.chunk import ChunkConfig
from docs_rag.config import Config
from docs_rag.dq import DRIFT_CHECKS, FAIL, DqResult, Totals, evaluate
from docs_rag.hashing import sha256_hex
from docs_rag.silver import BronzeDoc, SilverRecord, chunk_docs, to_records

D002_PATH = (("v3.5.0", None), ("v4.0.0", "v3.5.0"), ("v4.0.1", "v4.0.0"), ("v4.0.1", "v4.0.1"))
THRESHOLDS = (0.05, 0.10, 0.15, 0.20, 0.25, 0.30)
_RELEASE = re.compile(r"v(\d+)\.(\d+)\.(\d+)")


def release_tags(since: tuple[int, int, int]) -> list[str]:
    out = subprocess.run(
        ["git", "ls-remote", "--tags", SOURCE], capture_output=True, text=True, check=True
    ).stdout
    tags = {line.split("refs/tags/", 1)[1] for line in out.splitlines() if "refs/tags/" in line}
    releases = [t for t in tags if (m := _RELEASE.fullmatch(t)) and tuple(map(int, m.groups())) >= since]
    return sorted(releases, key=version)


def version(tag: str) -> tuple[int, int, int]:
    m = _RELEASE.fullmatch(tag)
    assert m, tag
    a, b, c = map(int, m.groups())
    return a, b, c


def kind(a: str, b: str) -> str:
    va, vb = version(a), version(b)
    return "major" if va[0] != vb[0] else "minor" if va[1] != vb[1] else "patch"


def records_of(tag: TagDocs, chunking: ChunkConfig, config: Config) -> list[SilverRecord]:
    docs = [
        BronzeDoc(p, c, sha256_hex(c.encode()), sha256_hex(c.encode()), tag.commit_sha)
        for p, c in tag.files.items()
    ]
    at = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)  # not used by any check
    return to_records(
        docs, chunk_docs(docs, chunking), config.source_repo, tag.tag, chunking, "calibration", at
    )


def totals(tag: TagDocs, records: list[SilverRecord]) -> Totals:
    return Totals(len(tag.files), len(records), sum(r.n_tokens for r in records))


def brief(r: DqResult) -> dict[str, Any]:
    return {
        "status": r.status,
        "numerator": r.numerator,
        "denominator": r.denominator,
        "observed": None if r.observed is None else round(r.observed, 6),
    }


def full(r: DqResult) -> dict[str, Any]:
    return {**brief(r), "threshold": r.threshold, "rule": r.rule, "examples": list(r.examples)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--since", default="v3.0.0")
    args = ap.parse_args()
    out_path = REPO / "runs" / f"{args.label}.json"
    if out_path.exists():
        raise SystemExit(f"runs/{args.label}.json exists: choose a new label")
    head = committed_head()
    redactor = Redactor(WorkspaceClient())  # fails now, not after the git loop, if the credentials are bad
    config = Config()
    tags = release_tags(version(args.since))
    print(len(tags), "release tags:", " ".join(tags), flush=True)

    # Every release under the published config, each against the previous one.
    per_tag: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    prev: tuple[str, Totals] | None = None
    for tag_name in tags:
        tag = read_tags([tag_name])[tag_name]
        try:
            records = records_of(tag, config.chunk, config)
        except Exception as e:  # noqa: BLE001 - an old tag the cleaner or chunker cannot handle is a finding
            per_tag.append(
                {"tag": tag_name, "commit": tag.commit_sha, "docs": len(tag.files), "error": repr(e)}
            )
            print(tag_name, "chunking failed:", repr(e)[:200], flush=True)
            prev = None
            continue
        now = totals(tag, records)
        results = evaluate(records, now.docs, prev[1] if prev else None, config, config.chunk, tag_name)
        checks = {r.check: brief(r) for r in results}
        per_tag.append(
            {
                "tag": tag_name,
                "commit": tag.commit_sha,
                "docs": now.docs,
                "chunks": now.chunks,
                "tokens": now.tokens,
                "baseline": prev[0] if prev else None,
                "checks": checks,
                "failing_checks": [c for c, v in checks.items() if v["status"] == FAIL],
            }
        )
        if prev:
            transitions.append(
                {
                    "from": prev[0],
                    "to": tag_name,
                    "kind": kind(prev[0], tag_name),
                    "drift": {c: checks[c]["observed"] for c in DRIFT_CHECKS},
                    "fails_at_max_drift": [c for c in DRIFT_CHECKS if checks[c]["status"] == FAIL],
                }
            )
        print(tag_name, now, per_tag[-1]["failing_checks"], flush=True)
        prev = (tag_name, now)

    firing: dict[str, dict[str, dict[str, int]]] = {}
    for t in THRESHOLDS:
        by_kind: dict[str, dict[str, int]] = {}
        for k in ("patch", "minor", "major", "all"):
            subset = [x for x in transitions if k in ("all", x["kind"])]
            by_kind[k] = {
                "transitions": len(subset),
                "any_metric_over": sum(any(abs(v) > t for v in x["drift"].values()) for x in subset),
                "docs_over": sum(abs(x["drift"]["drift_docs"]) > t for x in subset),
                "chunks_over": sum(abs(x["drift"]["drift_chunks"]) > t for x in subset),
                "tokens_over": sum(abs(x["drift"]["drift_tokens"]) > t for x in subset),
            }
        firing[f"{t:.2f}"] = by_kind

    # D-002's path, full results, under every config of the grid.
    path_tags = read_tags(sorted({t for t, _ in D002_PATH}))
    d002: dict[str, list[dict[str, Any]]] = {}
    for chunking in config.chunk_grid:
        recs = {t: records_of(path_tags[t], chunking, config) for t in path_tags}
        steps = []
        for to_tag, from_tag in D002_PATH:
            base = totals(path_tags[from_tag], recs[from_tag]) if from_tag else None
            results = evaluate(recs[to_tag], len(path_tags[to_tag].files), base, config, chunking, to_tag)
            steps.append({"from": from_tag, "to": to_tag, "checks": {r.check: full(r) for r in results}})
        d002[f"{chunking.max_tokens}/{chunking.overlap_tokens}"] = steps

    out = {
        "label": args.label,
        "git_commit": head,
        "published_config": {
            "max_tokens": config.chunk.max_tokens,
            "overlap_tokens": config.chunk.overlap_tokens,
            "config_id": config.chunk.config_id,
        },
        "dq_config": dataclasses.asdict(config.dq),
        "embedding_max_seq_length": config.embedding_max_seq_length,
        "tags": tags,
        "per_tag": per_tag,
        "transitions": transitions,
        "drift_firing_by_threshold": firing,
        "tags_failing_each_check": dict(Counter(c for t in per_tag for c in t.get("failing_checks", []))),
        "d002_path": d002,
    }
    out_path.write_text(redactor.dump(out))
    print("transitions:", len(transitions), "| firing at max_drift:", firing[f"{config.dq.max_drift:.2f}"])


if __name__ == "__main__":
    main()
