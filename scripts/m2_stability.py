"""Chunk-ID stability across tags, and the corpus numbers behind the M2 cleaning and chunking choices.

    uv run --with databricks-sdk==0.152.0 python scripts/m2_stability.py --label m2_stability \
        --tags v3.5.0 v4.0.0 v4.0.1

Reads every tag's docs from git (scripts/gitdocs.py, not the pipeline's fetch) and chunks them locally with
the default chunk config. Writes runs/<label>.json with:

- per transition (a -> b): docs added / deleted / modified / unchanged by content; for section-aware chunking
  and for a whole-page baseline (the same cleaner and packer, but the page is one section), how many of a's
  chunk IDs b keeps and how many new IDs b brings (what M5 would embed); for modified docs, what happened to
  each lost ID (its section was edited, renamed or moved, or removed);
- per tag: the overlap and lead-in behaviour of the chunker, and the same numbers with table-border
  shortening switched off (an ablation of that cleaning rule).

Local only: Databricks is used just to redact account identifiers, as every runs/ writer does.
"""

import argparse
import contextlib
import functools
import pathlib
import platform
import statistics
import subprocess
import sys
from collections import Counter
from collections.abc import Iterator
from typing import Any
from unittest import mock

from databricks.sdk import WorkspaceClient
from gitdocs import TagDocs, read_tags
from redact import Redactor

from docs_rag.chunk import ChunkConfig, DocChunks, chunk_document, chunk_id, chunk_section
from docs_rag.clean import clean_page
from docs_rag.config import Config, validate_tag
from docs_rag.sections import split_sections
from docs_rag.tokens import count_tokens

REPO = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True).stdout.strip()


def stem(path: str) -> str:
    return path.rsplit("/", 1)[-1].removesuffix(".md")


def section_ids(tag: TagDocs, config: ChunkConfig) -> dict[str, DocChunks]:
    return {p: chunk_document(p, c, config) for p, c in sorted(tag.files.items())}


@functools.cache
def whole_page_ids(path: str, content: str, config: ChunkConfig) -> list[str]:
    """Baseline: the same cleaning and packing, but the whole page is one section (no heading boundaries)."""
    page = clean_page(content, fallback_title=stem(path))
    seen: Counter[str] = Counter()
    ids = []
    for _body, embed_text in chunk_section(path, (page.title,), page.text, config):
        ids.append(chunk_id(path, (page.title,), embed_text, seen[embed_text]))
        seen[embed_text] += 1
    return ids


def sections_of(path: str, content: str) -> Counter[tuple[tuple[str, ...], str]]:
    page = clean_page(content, fallback_title=stem(path))
    return Counter((s.path, s.body) for s in split_sections(page.title, page.text) if s.body)


def located(section_path: tuple[str, ...], text: str, sections: Counter[tuple[tuple[str, ...], str]]) -> Any:
    """The body of the section a chunk comes from: the chunk is a slice of it, or a re-fenced slice of it."""
    inner = "\n".join(text.split("\n")[1:-1]) if text.startswith(("```", "~~~")) else None
    for path, body in sections:
        if path == section_path and (text in body or (inner is not None and inner in body)):
            return body
    return None


def transition(a: TagDocs, b: TagDocs, config: ChunkConfig, chunked: dict[str, dict[str, DocChunks]]) -> Any:
    common = sorted(a.files.keys() & b.files.keys())
    modified = [p for p in common if a.files[p] != b.files[p]]
    unchanged = [p for p in common if a.files[p] == b.files[p]]
    out: dict[str, Any] = {
        "docs": {
            "added": len(b.files.keys() - a.files.keys()),
            "deleted": len(a.files.keys() - b.files.keys()),
            "modified": len(modified),
            "unchanged": len(unchanged),
        }
    }
    ca, cb = chunked[a.tag], chunked[b.tag]
    methods = {
        "section_aware": (lambda tag, p: [c.chunk_id for c in (ca if tag == a.tag else cb)[p].chunks]),
        "whole_page_baseline": (
            lambda tag, p: whole_page_ids(p, (a if tag == a.tag else b).files[p], config)
        ),
    }
    for name, ids_of in methods.items():
        kept = removed = in_a = 0
        same_set_modified = 0
        for p in common:
            ia, ib = Counter(ids_of(a.tag, p)), Counter(ids_of(b.tag, p))
            in_a += sum(ia.values())
            kept += sum((ia & ib).values())
            removed += sum((ia - ib).values())
            if p in modified and ia == ib:
                same_set_modified += 1
        new_in_b = sum(
            sum((Counter(ids_of(b.tag, p)) - Counter(ids_of(a.tag, p) if p in a.files else [])).values())
            for p in b.files
        )
        unchanged_docs_same_ids = sum(
            Counter(ids_of(a.tag, p)) == Counter(ids_of(b.tag, p)) for p in unchanged
        )
        out[name] = {
            "chunks_in_a_for_common_docs": in_a,
            "kept": kept,
            "removed": removed,
            "kept_share": round(kept / in_a, 4) if in_a else None,
            "new_ids_in_b_to_embed": new_in_b,
            "chunks_in_b": sum(len(ids_of(b.tag, p)) for p in b.files),
            "modified_docs_with_unchanged_chunk_set": same_set_modified,
            "unchanged_docs_with_identical_chunk_ids": f"{unchanged_docs_same_ids}/{len(unchanged)}",
        }
    # Where the section-aware IDs lost in modified docs went.
    buckets: Counter[str] = Counter()
    sections_unchanged = sections_unchanged_kept = sections_unchanged_chunks = 0
    for p in modified:
        sa, sb = sections_of(p, a.files[p]), sections_of(p, b.files[p])
        paths_b = {path for path, _ in sb}
        bodies_b = {body for _, body in sb}
        ids_b = Counter(c.chunk_id for c in cb[p].chunks)
        for c in ca[p].chunks:
            body = located(c.section_path, c.body, sa)
            if body is None:
                buckets["chunk not located in its section (check the script)"] += 1
                continue
            in_b = ids_b[c.chunk_id] > 0
            if (c.section_path, body) in sb:
                sections_unchanged_chunks += 1
                sections_unchanged_kept += in_b
            if in_b:
                continue
            if (c.section_path, body) in sb:
                buckets["section unchanged (should not happen)"] += 1
            elif c.section_path in paths_b:
                buckets["section text edited"] += 1
            elif body in bodies_b:
                buckets["section renamed or moved, text unchanged"] += 1
            else:
                buckets["section removed or rewritten under a new heading"] += 1
        sections_unchanged += sum((sa & sb).values())
    out["section_aware_modified_docs"] = {
        "unchanged_sections": sections_unchanged,
        "chunks_in_unchanged_sections": sections_unchanged_chunks,
        "of_which_kept": sections_unchanged_kept,
        "lost_ids_by_cause": dict(buckets),
    }
    return out


def chunker_behaviour(tag: TagDocs, config: ChunkConfig, chunked: dict[str, DocChunks]) -> Any:
    """Overlap at cuts inside a paragraph, and lead-ins left at the end of a chunk."""
    stats: Counter[str] = Counter()
    for p, doc in chunked.items():
        page = clean_page(tag.files[p], fallback_title=stem(p))
        bodies: dict[tuple[str, ...], list[str]] = {}
        for s in split_sections(page.title, page.text):
            bodies.setdefault(s.path, []).append(s.body)
        for left, right in zip(doc.chunks, doc.chunks[1:], strict=False):
            if left.section_path != right.section_path:
                continue
            stats["continuations"] += 1
            last = left.body.rsplit("\n\n", 1)[-1]
            if "\n" not in last and last.rstrip().endswith(":"):
                stats["ending_in_a_one_line_lead_in"] += 1
                stats["of_which_the_next_chunk_repeats_it"] += right.body.startswith(last)
            if left.body.endswith("```") or right.body.startswith(("```", "~~~")):
                stats["at_a_code_block"] += 1
                continue
            body = next((b for b in bodies[left.section_path] if left.body in b and right.body in b), None)
            if body is None:
                stats["not_located"] += 1
                continue
            end = body.find(left.body) + len(left.body)
            start = body.find(right.body, body.find(left.body) + 1)
            if start < end:
                stats["prose_with_overlap"] += 1
            elif "\n\n" in body[end:start]:
                stats["prose_no_overlap_at_paragraph_break"] += 1
            else:
                stats["prose_no_overlap_inside_paragraph"] += 1
    return dict(stats)


def corpus_numbers(tag: TagDocs, config: ChunkConfig) -> dict[str, Any]:
    docs = section_ids(tag, config)
    n = [c.n_tokens for d in docs.values() for c in d.chunks]
    border_tokens = 0
    total_tokens = 0
    for p, content in tag.files.items():
        text = clean_page(content, fallback_title=stem(p)).text
        total_tokens += count_tokens(text)
        border_tokens += sum(
            count_tokens(line) for line in text.split("\n") if line.strip() and set(line) <= set("|+:=- \t")
        )
    return {
        "docs": len(docs),
        "chunks": len(n),
        "n_tokens_median": statistics.median(n),
        "n_tokens_max": max(n),
        "cleaned_tokens": total_tokens,
        "tokens_on_border_only_lines": border_tokens,
        "docs_with_no_chunks": sorted(p for p, d in docs.items() if not d.chunks),
        "behaviour": chunker_behaviour(tag, config, docs),
    }


@contextlib.contextmanager
def rule_off(name: str) -> Iterator[None]:
    if name == "border_shortening":
        with mock.patch("docs_rag.clean._shorten_borders", lambda line: line):
            yield
    else:
        raise ValueError(name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True, help="tags in release order")
    args = ap.parse_args()
    dirty = ["git", "-C", REPO, "status", "--porcelain", "--", ".", ":(exclude)runs"]
    if subprocess.run(dirty, capture_output=True, text=True).stdout:
        sys.exit("uncommitted code changes (runs/ excepted): commit first, so the result can be traced")
    tags = [validate_tag(t) for t in args.tags]
    config = Config().chunk
    source = read_tags(tags)
    chunked = {t: section_ids(source[t], config) for t in tags}
    out: dict[str, Any] = {
        "label": args.label,
        "git_commit": subprocess.run(
            ["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "chunk_config": {**config.__dict__, "config_id": config.config_id},
        "source": {t: {"commit_sha": source[t].commit_sha, "docs": len(source[t].files)} for t in tags},
        "transitions": {
            f"{a}->{b}": transition(source[a], source[b], config, chunked)
            for a, b in zip(tags, tags[1:], strict=False)
        },
        "tags": {t: corpus_numbers(source[t], config) for t in tags},
        "ablations": {},
    }
    for rule in ("border_shortening",):
        with rule_off(rule):
            out["ablations"][f"{rule}_off"] = {t: corpus_numbers(source[t], config) for t in tags}
    text = Redactor(WorkspaceClient()).dump(out)
    (pathlib.Path(REPO) / "runs" / f"{args.label}.json").write_text(text)
    print(text[:6000])


if __name__ == "__main__":
    main()
