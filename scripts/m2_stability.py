"""Chunk-ID stability across tags, and the corpus numbers behind the M2 cleaning and chunking choices.

    uv run --with databricks-sdk==0.152.0 python scripts/m2_stability.py --label m2_stability \
        --tags v3.5.0 v4.0.0 v4.0.1

Reads every tag's docs from git (scripts/gitdocs.py, not the pipeline's fetch) and chunks them locally with
every config of Config.chunk_grid. Writes runs/<label>.json with, per config:

- per transition (a -> b): docs added / deleted / modified / unchanged by content. For section-aware chunking
  and for a whole-page baseline (the same cleaner and packer, but the page is one section): how many of a's
  chunk IDs, and of a's tokens, b keeps, and how many new IDs and tokens b brings (what M5 would embed);
- for the modified docs: what happened to each lost section-aware ID (own text edited, a boundary shift,
  a frame edit, a move under another heading, a removal), and, as a locality check, how many chunks lying
  wholly inside an unchanged section each method keeps;
- per tag: chunk counts and where consecutive chunks meet (overlap), and the same numbers with one rule
  switched off at a time (ablations): table-border shortening (cleaning) and one-chunk-per-row tables.

Local only: Databricks is used just to redact account identifiers, as every runs/ writer does.
"""

import argparse
import contextlib
import functools
import platform
import statistics
from collections import Counter
from collections.abc import Iterator
from typing import Any
from unittest import mock

from databricks.sdk import WorkspaceClient
from gitdocs import TagDocs, read_tags
from redact import Redactor
from runguard import REPO, committed_head

from docs_rag.chunk import Chunk, ChunkConfig, DocChunks, chunk_document, chunk_id, chunk_section, rebuilds
from docs_rag.clean import clean_page
from docs_rag.config import Config, validate_tag
from docs_rag.sections import Section, split_sections
from docs_rag.tokens import count_tokens


def stem(path: str) -> str:
    return path.rsplit("/", 1)[-1].removesuffix(".md")


@functools.cache
def clean_text(path: str, content: str) -> str:
    return clean_page(content, fallback_title=stem(path)).text


def section_chunks(tag: TagDocs, config: ChunkConfig) -> dict[str, DocChunks]:
    return {p: chunk_document(p, c, config) for p, c in sorted(tag.files.items())}


@functools.cache
def whole_page_chunks(path: str, content: str, config: ChunkConfig) -> list[tuple[str, int, int, int]]:
    """Baseline: the same cleaning and packing, but the whole page is one section (no heading boundaries).
    (chunk_id, n_tokens, char_start, char_end) per chunk."""
    page = clean_page(content, fallback_title=stem(path))
    seen: Counter[str] = Counter()
    out = []
    for sc in chunk_section(path, (page.title,), page.text, config):
        out.append(
            (chunk_id(path, (page.title,), sc.embed_text, seen[sc.embed_text]), count_tokens(sc.embed_text))
            + (sc.start, sc.end)
        )
        seen[sc.embed_text] += 1
    return out


def sections_of(path: str, content: str) -> list[Section]:
    page = clean_page(content, fallback_title=stem(path))
    return [s for s in split_sections(page.title, page.text) if s.body]


def inside(sections: list[Section], start: int, end: int) -> Section | None:
    return next((s for s in sections if s.start <= start and end <= s.start + len(s.body)), None)


def framed(c: Chunk) -> bool:
    """A re-fenced code piece or a table row under its header: the body is not the bare slice."""
    return c.frame != "none"


def whole_lines_in(text: str, body: str) -> bool:
    """Every line of text is a whole line of body: the text itself is unchanged there."""
    lines = set(body.split("\n"))
    return all(line in lines for line in text.split("\n"))


def method_numbers(
    a: TagDocs, b: TagDocs, ids_of: Any, common: list[str], modified: list[str], unchanged: list[str]
) -> dict[str, Any]:
    """ids_of(tag, path) -> [(chunk_id, n_tokens)]."""
    kept = removed = in_a = kept_tokens = tokens_a = same_set_modified = 0
    for p in common:
        ca, cb = ids_of(a, p), ids_of(b, p)
        ia, ib = Counter(i for i, _ in ca), Counter(i for i, _ in cb)
        in_a += len(ca)
        tokens_a += sum(n for _, n in ca)
        both = ia & ib
        kept += sum(both.values())
        removed += sum((ia - ib).values())
        left = Counter(both)
        for i, n in ca:  # tokens of the kept chunks of a
            if left[i]:
                left[i] -= 1
                kept_tokens += n
        same_set_modified += p in modified and ia == ib
    new_ids = new_tokens = 0
    for p in b.files:
        old = Counter(i for i, _ in ids_of(a, p)) if p in a.files else Counter()
        for i, n in ids_of(b, p):
            if old[i]:
                old[i] -= 1
            else:
                new_ids += 1
                new_tokens += n
    unchanged_same = sum(Counter(ids_of(a, p)) == Counter(ids_of(b, p)) for p in unchanged)
    return {
        "chunks_in_a_for_common_docs": in_a,
        "kept": kept,
        "removed": removed,
        "kept_share": round(kept / in_a, 4) if in_a else None,
        "kept_token_share": round(kept_tokens / tokens_a, 4) if tokens_a else None,
        "new_ids_in_b_to_embed": new_ids,
        "new_tokens_in_b_to_embed": new_tokens,
        "chunks_in_b": sum(len(ids_of(b, p)) for p in b.files),
        "tokens_in_b": sum(n for p in b.files for _, n in ids_of(b, p)),
        "modified_docs_with_unchanged_chunk_set": same_set_modified,
        "unchanged_docs_with_identical_chunk_ids": f"{unchanged_same}/{len(unchanged)}",
    }


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

    def section_ids(tag: TagDocs, p: str) -> list[tuple[str, int]]:
        return [(c.chunk_id, c.n_tokens) for c in chunked[tag.tag][p].chunks]

    def page_ids(tag: TagDocs, p: str) -> list[tuple[str, int]]:
        return [(i, n) for i, n, _, _ in whole_page_chunks(p, tag.files[p], config)]

    out["section_aware"] = method_numbers(a, b, section_ids, common, modified, unchanged)
    out["whole_page_baseline"] = method_numbers(a, b, page_ids, common, modified, unchanged)

    # Modified docs: where each lost section-aware ID went, and locality (chunks inside unchanged sections).
    buckets: Counter[str] = Counter()
    local = Counter[str]()
    for p in modified:
        sa, sb = sections_of(p, a.files[p]), sections_of(p, b.files[p])
        unchanged_b = Counter((s.path, s.body) for s in sb)
        text_a = clean_text(p, a.files[p])
        ids_b = Counter(c.chunk_id for c in chunked[b.tag][p].chunks)
        for c in chunked[a.tag][p].chunks:
            section = inside(sa, c.char_start, c.char_end)
            assert section is not None, (p, c.chunk_index)
            in_b = ids_b[c.chunk_id] > 0
            if (section.path, section.body) in unchanged_b:
                local["section_aware_chunks_inside_unchanged_sections"] += 1
                local["section_aware_kept"] += in_b
            if in_b:
                continue
            own = text_a[c.char_start : c.char_end]  # the chunk's own text: no breadcrumb, no frame
            same_path = [s.body for s in sb if s.path == c.section_path]
            kind = {"none": "prose", "table": "table row", "code": "code piece"}[c.frame]
            heads_b = {x.frame_head for x in chunked[b.tag][p].chunks if x.section_path == c.section_path}
            if (section.path, section.body) in unchanged_b:
                cause = "section unchanged (should not happen)"
            elif any(whole_lines_in(own, body) for body in same_path):
                # Own text survives: its frame (table header, code fence) changed, or it is cut differently.
                cause = (
                    "own text unchanged, frame edited"
                    if framed(c) and c.frame_head not in heads_b
                    else "boundary shift"
                )
            elif same_path:
                cause = "own text edited"
            elif section.body in {s.body for s in sb}:
                cause = "breadcrumb rename: section unchanged under a new heading path"
            elif any(whole_lines_in(own, s.body) for s in sb):
                cause = "moved under another heading"
            else:
                cause = "section removed or rewritten"
            buckets[f"{kind}: {cause}"] += 1
        ids_b_page = Counter(i for i, _, _, _ in whole_page_chunks(p, b.files[p], config))
        for i, _, start, end in whole_page_chunks(p, a.files[p], config):
            section = inside(sa, start, end)
            if section is not None and (section.path, section.body) in unchanged_b:
                local["baseline_chunks_inside_unchanged_sections"] += 1
                local["baseline_kept"] += ids_b_page[i] > 0
    out["modified_docs"] = {"lost_section_aware_ids_by_cause": dict(buckets), "locality": dict(local)}
    return out


def chunker_behaviour(tag: TagDocs, chunked: dict[str, DocChunks]) -> Any:
    """What happens where one chunk ends and the next begins in the same section, from the chunk offsets."""
    stats: Counter[str] = Counter()
    for p, doc in chunked.items():
        text = clean_text(p, tag.files[p])
        for left, right in zip(doc.chunks, doc.chunks[1:], strict=False):
            if left.section_path != right.section_path:
                continue
            stats["continuations"] += 1
            if framed(left) or framed(right):
                overlap = right.char_start < left.char_end  # only within one split code block or one long row
                stats[f"at_a_split_code_block_or_table_row_{'with' if overlap else 'without'}_overlap"] += 1
            elif left.body.endswith(("```", "~~~")) or right.body.startswith(("```", "~~~")):
                stats["at_a_whole_code_block"] += 1
            elif right.char_start < left.char_end:
                stats["prose_with_overlap"] += 1
            elif "\n\n" in text[left.char_end : right.char_start]:
                stats["prose_no_overlap_at_paragraph_break"] += 1
            else:
                stats["prose_no_overlap_inside_paragraph"] += 1
    return dict(stats)


def corpus_numbers(tag: TagDocs, config: ChunkConfig) -> dict[str, Any]:
    docs = section_chunks(tag, config)
    n = [c.n_tokens for d in docs.values() for c in d.chunks]
    border_tokens = total_tokens = 0
    frames: Counter[str] = Counter()
    for p, content in tag.files.items():
        text = clean_page(content, fallback_title=stem(p)).text  # not cached: ablations patch the cleaner
        total_tokens += count_tokens(text)
        border_tokens += sum(
            count_tokens(line) for line in text.split("\n") if line.strip() and set(line) <= set("|+:=- \t")
        )
        frames.update(c.frame if rebuilds(c, text) else "mismatch" for c in docs[p].chunks)
    return {
        "docs": len(docs),
        "chunks": len(n),
        "chunks_by_frame": dict(frames),
        "embedded_tokens": sum(n),
        "n_tokens_median": statistics.median(n),
        "n_tokens_max": max(n),
        "cleaned_tokens": total_tokens,
        "tokens_on_border_only_lines": border_tokens,
        "docs_with_no_chunks": sorted(p for p, d in docs.items() if not d.chunks),
    }


@contextlib.contextmanager
def rule_off(name: str) -> Iterator[None]:
    if name == "border_shortening":
        with mock.patch("docs_rag.clean._shorten_borders", lambda line: line):
            yield
    elif name == "table_rows":
        with mock.patch("docs_rag.chunk._table_rows", lambda body, unit: None):
            yield
    else:
        raise ValueError(name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--tags", nargs="+", required=True, help="tags in release order")
    args = ap.parse_args()
    head = committed_head()
    tags = [validate_tag(t) for t in args.tags]
    source = read_tags(tags)
    out: dict[str, Any] = {
        "label": args.label,
        "git_commit": head,
        "machine": platform.machine(),
        "python": platform.python_version(),
        "source": {t: {"commit_sha": source[t].commit_sha, "docs": len(source[t].files)} for t in tags},
        "configs": {},
    }
    for config in Config().chunk_grid:
        chunked = {t: section_chunks(source[t], config) for t in tags}
        numbers: dict[str, Any] = {
            "chunk_config": {**config.__dict__, "config_id": config.config_id},
            "transitions": {
                f"{a}->{b}": transition(source[a], source[b], config, chunked)
                for a, b in zip(tags, tags[1:], strict=False)
            },
            "tags": {
                t: {
                    **corpus_numbers(source[t], config),
                    "behaviour": chunker_behaviour(source[t], chunked[t]),
                }
                for t in tags
            },
            "ablations": {},
        }
        for rule in ("border_shortening", "table_rows"):
            with rule_off(rule):
                numbers["ablations"][f"{rule}_off"] = {t: corpus_numbers(source[t], config) for t in tags}
        out["configs"][f"{config.max_tokens}/{config.overlap_tokens}"] = numbers
    text = Redactor(WorkspaceClient()).dump(out)
    (REPO / "runs" / f"{args.label}.json").write_text(text)
    print(text[:4000])


if __name__ == "__main__":
    main()
