"""Retrieval evaluation (D-015): the eval set, relevance by quote overlap, ranking metrics, paired bootstrap.

An item labels its answer as a verbatim quote of the cleaned text (D-010), not as chunk ids, so one label
serves every chunk config. A chunk is relevant to an item when its slice of the cleaned text covers at least
a share (EvalConfig.min_quote_share, one half) of some occurrence of the quote, anywhere in the corpus
(language tabs and shared pages repeat text). Where chunks overlap, a quote that straddles a boundary still
has a chunk covering at least half of it; where two meet at a paragraph break without overlap it may not, so
the eval checks that every item has a relevant chunk under every config rather than assuming it.

recall@k is the share of items with at least one relevant chunk in the top k (one answer per item, so a chunk
config that splits the answer across chunks is not penalised for it); MRR@k averages 1/rank of the first
relevant chunk, 0 beyond k. Uncertainty: percentile bootstrap over items with one seeded set of resamples,
shared by every config, so differences between configs are paired. Pure Python.
"""

import json
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class EvalItem:
    item_id: str
    question: str
    answer: str
    path: str
    section_path: tuple[str, ...]
    quote: str
    kind: str
    also_in: tuple[str, ...]  # other docs that state the answer in other words (counted as misses)


@dataclass(frozen=True)
class EvalSet:
    version: str
    tag: str
    commit_sha: str
    cleaner_version: str
    items: tuple[EvalItem, ...]


def load_eval_set(text: str) -> EvalSet:
    data = json.loads(text)
    items = tuple(
        EvalItem(
            item_id=i["item_id"],
            question=i["question"],
            answer=i["answer"],
            path=i["path"],
            section_path=tuple(i["section_path"]),
            quote=i["quote"],
            kind=i["kind"],
            also_in=tuple(i.get("also_in", [])),
        )
        for i in data["items"]
    )
    if len({i.item_id for i in items}) != len(items):
        raise ValueError("item ids are not unique")
    return EvalSet(data["version"], data["tag"], data["commit_sha"], data["cleaner_version"], items)


@dataclass(frozen=True)
class Occurrence:
    path: str
    start: int
    end: int


def occurrences(quote: str, clean_texts: Mapping[str, str]) -> list[Occurrence]:
    """Every occurrence of the quote in every doc's cleaned text, overlapping ones included."""
    found = []
    for path, text in sorted(clean_texts.items()):
        at = text.find(quote)
        while at != -1:
            found.append(Occurrence(path, at, at + len(quote)))
            at = text.find(quote, at + 1)
    return found


@dataclass(frozen=True)
class ChunkSpan:
    chunk_id: str
    path: str
    start: int  # the chunk's slice of the cleaned text (silver char_start / char_end)
    end: int


def relevant_ids(
    found: Sequence[Occurrence], chunks: Sequence[ChunkSpan], min_share: float
) -> frozenset[str]:
    by_path: dict[str, list[Occurrence]] = {}
    for o in found:
        by_path.setdefault(o.path, []).append(o)
    out = set()
    for c in chunks:
        for o in by_path.get(c.path, []):
            covered = min(c.end, o.end) - max(c.start, o.start)
            if covered >= min_share * (o.end - o.start):
                out.add(c.chunk_id)
                break
    return frozenset(out)


def item_violations(
    item: EvalItem,
    clean_texts: Mapping[str, str],
    section_spans: Mapping[tuple[str, ...], Sequence[tuple[int, int]]],
    quote_chars: tuple[int, int],
) -> list[str]:
    """What makes an item unusable: a quote that is not where the item says (in its doc, inside its section)
    or of the wrong length. `section_spans` maps the doc's section paths to their (start, end) offsets (a
    heading path can repeat within a doc)."""
    problems = []
    if not quote_chars[0] <= len(item.quote) <= quote_chars[1]:
        problems.append(f"quote of {len(item.quote)} characters, outside {quote_chars}")
    found = occurrences(item.quote, {item.path: clean_texts.get(item.path, "")})
    spans = section_spans.get(item.section_path)
    if not spans:
        problems.append(f"no section {item.section_path} in {item.path}")
    elif not any(start <= o.start and o.end <= end for o in found for start, end in spans):
        problems.append("the quote does not occur in its section")
    if not item.question.strip() or not item.answer.strip():
        problems.append("empty question or answer")
    return problems


def longest_shared_run(question: str, quote: str) -> int:
    """The longest run of consecutive words the question shares with the quote (case-folded): a high value
    means retrieval can succeed by string match."""
    q, t = question.lower().split(), quote.lower().split()
    best = 0
    for i in range(len(q)):
        for j in range(len(t)):
            n = 0
            while (
                i + n < len(q) and j + n < len(t) and q[i + n].strip(".,?;:()") == t[j + n].strip(".,?;:()")
            ):
                n += 1
            best = max(best, n)
    return best


# --- metrics ---------------------------------------------------------------------------------------------


def first_relevant_rank(ranked: Sequence[str], relevant: frozenset[str]) -> int | None:
    """1-based rank of the first relevant chunk in the ranking, or None if there is none."""
    return next((i + 1 for i, c in enumerate(ranked) if c in relevant), None)


def hit_at(rank: int | None, k: int) -> float:
    return 1.0 if rank is not None and rank <= k else 0.0


def reciprocal_rank_at(rank: int | None, k: int) -> float:
    return 1.0 / rank if rank is not None and rank <= k else 0.0


def resamples(n_items: int, n_resamples: int, seed: int) -> list[list[int]]:
    """Bootstrap resamples of item positions, drawn once and shared by every config (paired)."""
    rng = random.Random(seed)
    return [[rng.randrange(n_items) for _ in range(n_items)] for _ in range(n_resamples)]


def _percentile(sorted_values: Sequence[float], q: float) -> float:
    """Linear interpolation between closest ranks."""
    pos = q * (len(sorted_values) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


@dataclass(frozen=True)
class Estimate:
    mean: float
    low: float  # 2.5th percentile of the bootstrap means
    high: float  # 97.5th


def bootstrap(values: Sequence[float], draws: Sequence[Sequence[int]]) -> Estimate:
    means = sorted(sum(values[i] for i in d) / len(d) for d in draws)
    return Estimate(sum(values) / len(values), _percentile(means, 0.025), _percentile(means, 0.975))


@dataclass(frozen=True)
class Difference:
    mean: float  # mean of a - b
    low: float
    high: float
    share_above_zero: float  # share of resamples where a beat b
    a_only: int  # items a got right (hit) and b did not
    b_only: int


def paired_difference(a: Sequence[float], b: Sequence[float], draws: Sequence[Sequence[int]]) -> Difference:
    if len(a) != len(b):
        raise ValueError("paired values of different lengths")
    diffs = [x - y for x, y in zip(a, b, strict=True)]
    means = sorted(sum(diffs[i] for i in d) / len(d) for d in draws)
    return Difference(
        mean=sum(diffs) / len(diffs),
        low=_percentile(means, 0.025),
        high=_percentile(means, 0.975),
        share_above_zero=sum(m > 0 for m in means) / len(means),
        a_only=sum(x > y for x, y in zip(a, b, strict=True)),
        b_only=sum(y > x for x, y in zip(a, b, strict=True)),
    )


# --- the eval_results row ---------------------------------------------------------------------------

EVAL_RESULTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("eval_run_id", "STRING NOT NULL"),
    ("eval_set_version", "STRING NOT NULL"),
    ("eval_set_sha256", "STRING NOT NULL"),
    ("item_id", "STRING NOT NULL"),
    ("model_id", "STRING NOT NULL"),  # model@revision
    ("query_prefix", "STRING NOT NULL"),  # '' when none
    ("config_id", "STRING NOT NULL"),
    ("max_tokens", "INT NOT NULL"),
    ("overlap_tokens", "INT NOT NULL"),
    ("version_tag", "STRING NOT NULL"),
    ("vectors", "STRING NOT NULL"),  # 'local' (embedded by the eval) or 'gold' (read from the gold table)
    ("rank_first_relevant", "INT"),  # 1-based, over the full exact ranking; NULL if no relevant chunk ranks
    ("hit_at_k", "DOUBLE NOT NULL"),
    ("rr_at_k", "DOUBLE NOT NULL"),
    ("recall_k", "INT NOT NULL"),
    ("mrr_k", "INT NOT NULL"),
    ("top_chunk_ids", "ARRAY<STRING> NOT NULL"),  # the mrr_k best, in order
    ("relevant_chunk_ids", "ARRAY<STRING> NOT NULL"),
    ("evaluated_at", "TIMESTAMP NOT NULL"),
    ("code_commit", "STRING NOT NULL"),
)
EVAL_RESULTS_KEY = ("eval_run_id", "item_id", "model_id", "query_prefix", "config_id", "vectors")


def eval_results_ddl(nullable: bool = False) -> str:
    return ", ".join(f"{n} {k.removesuffix(' NOT NULL') if nullable else k}" for n, k in EVAL_RESULTS_COLUMNS)
