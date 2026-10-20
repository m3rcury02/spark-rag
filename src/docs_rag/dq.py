"""The DQ gate between silver and gold (D-012): checks on the target snapshot before a run may commit.

Brief item 5: empty or tiny chunks, oversized chunks, duplicate chunks, missing metadata, and row-count drift
between runs. Each check gives one DqResult: what it counted, out of what, its threshold, and up to
MAX_EXAMPLES examples. A count check fails when numerator > threshold * denominator (a threshold of 0: any).
A drift check compares the target's docs, chunks and tokens with the baseline run's and fails when
|to - from| > threshold * from, the threshold set by the release kind (patch, or minor and major;
changes.release_kind).
Statuses:
- pass / fail;
- overridden: a drift check over its threshold that this run accepts (an Acceptance): the operator names a
  run that failed the gate on drift alone, for the same target, baseline and config, and gives a reason; a
  check is overridden only if it fails with exactly the numbers that run logged. Only drift can be
  overridden, and a loss beyond max_override_loss cannot;
- not_applicable: drift of an initial run, which has no baseline.
The checks cover all the target's chunks, not only the added ones, because gold holds the whole target.
`gate` turns the results into a GatePassed, which committing a run requires (and, from M5, publishing it), or
raises DqFailed. Pure Python, so the job, the calibration script and the tests share it.
"""

import datetime as dt
import math
import re
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field

from docs_rag.changes import release_kind
from docs_rag.chunk import ChunkConfig
from docs_rag.config import Config, DqConfig
from docs_rag.silver import SilverRecord
from docs_rag.tokens import SPECIAL_TOKENS, count_tokens

PASS = "pass"
FAIL = "fail"
OVERRIDDEN = "overridden"
NOT_APPLICABLE = "not_applicable"
MAX_EXAMPLES = 10
DRIFT_CHECKS = ("drift_docs", "drift_chunks", "drift_tokens")
CHECKS = (
    "empty_chunks",
    "tiny_chunks",
    "oversized_chunks",
    "duplicate_chunk_ids",
    "duplicate_embed_text",
    "missing_metadata",
    *DRIFT_CHECKS,
)
_COMMIT = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class Totals:
    """What drift compares: a snapshot's docs (with or without chunks), chunks, and the chunks' tokens."""

    docs: int
    chunks: int
    tokens: int


@dataclass(frozen=True)
class Baseline:
    """The committed run a target drifts from: its tag (for the release kind) and its totals."""

    tag: str
    totals: Totals


@dataclass(frozen=True)
class LoggedCheck:
    """One ops_dq_results row of an earlier run, as an acceptance reads it."""

    run_id: str
    attempt_id: str
    check: str
    status: str
    numerator: int
    denominator: int
    config_id: str
    from_version_tag: str | None
    to_version_tag: str
    baseline_run_id: str | None


@dataclass(frozen=True)
class Acceptance:
    """Drift this run accepts: the failed run whose drift the operator saw, the drift checks it failed with
    their (check, numerator, denominator), and the operator's reason."""

    reason: str
    run_id: str
    drift: frozenset[tuple[str, int, int]]


@dataclass(frozen=True)
class DqResult:
    check: str
    status: str
    numerator: int  # chunks that break the rule; for drift, the target's total
    denominator: int  # chunks checked; for drift, the baseline's total (0 when there is none)
    observed: float | None  # numerator / denominator, for drift that minus 1; None when undefined
    threshold: float
    rule: str  # the rule in words, thresholds included
    examples: tuple[str, ...] = ()
    override_reason: str | None = None
    override_of: str | None = None  # the failed run whose drift this run accepted


class DqFailed(RuntimeError):
    """The gate refused the run: nothing may be committed or published."""


_ISSUED = object()


@dataclass(frozen=True)
class GatePassed:
    """Issued only by `gate`: every check passed, or a drift check was overridden by an acceptance."""

    checks: tuple[str, ...]
    overridden: tuple[str, ...]
    override_reason: str | None  # set exactly when a check was overridden
    override_of: str | None  # the failed run whose drift was accepted; set with override_reason
    issued: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.issued is not _ISSUED:
            raise TypeError("a GatePassed is issued only by docs_rag.dq.gate")


def validate_override(
    reason: str | None, of_run: str | None, initial: bool, dq: DqConfig
) -> tuple[str, str] | None:
    """--accept-drift (the reason, whitespace normalised) and --accept-drift-of (the failed run whose drift it
    accepts), checked before the run starts. Both or neither."""
    if reason is None and of_run is None:
        return None
    if reason is None or of_run is None or not of_run.strip():
        raise ValueError(
            "--accept-drift and --accept-drift-of go together: a reason, and the failed run whose drift "
            "it accepts"
        )
    if initial:
        raise ValueError("--accept-drift with --initial: an initial run has no baseline to drift from")
    text = " ".join(reason.split())
    if len(text) < dq.min_reason_chars or len(text.split()) < 4:
        raise ValueError(
            f"--accept-drift needs a reason of at least {dq.min_reason_chars} characters and 4 words, "
            f"got {reason!r}"
        )
    return text, of_run.strip()


def accept_failed_run(
    reason: str,
    run_id: str,
    statuses: Sequence[str],
    rows: Sequence[LoggedCheck],
    *,
    config_id: str,
    from_tag: str | None,
    tag: str,
    baseline_run_id: str | None,
) -> Acceptance:
    """Bind an override to the run named by --accept-drift-of: it has exactly one run-log row, failed, and it
    failed the gate on drift alone, in one attempt, for the same target, baseline and config as this run.
    `statuses` are its run-log statuses and `rows` its ops_dq_results rows."""
    problems = []
    if sorted(statuses) != ["failed"]:
        problems.append(f"its run log statuses are {sorted(statuses)}, not one failed row")
    attempts = {r.attempt_id for r in rows}
    if len(attempts) != 1:
        problems.append(f"it has DQ rows from {len(attempts)} attempts (it must have reached the gate once)")
    keys = {(r.config_id, r.from_version_tag, r.to_version_tag, r.baseline_run_id) for r in rows}
    if keys and keys != {(config_id, from_tag, tag, baseline_run_id)}:
        problems.append(
            f"it checked {sorted(keys, key=str)}, "
            f"this run checks {(config_id, from_tag, tag, baseline_run_id)}"
        )
    failed = [r for r in rows if r.status == FAIL]
    if rows and not [r for r in failed if r.check in DRIFT_CHECKS]:
        problems.append("it failed no drift check")
    if [r.check for r in failed if r.check not in DRIFT_CHECKS]:
        problems.append(f"it also failed {[r.check for r in failed if r.check not in DRIFT_CHECKS]}")
    if problems:
        raise ValueError(f"--accept-drift-of {run_id}: " + "; ".join(problems))
    return Acceptance(reason, run_id, frozenset((r.check, r.numerator, r.denominator) for r in failed))


def _where(r: SilverRecord) -> str:
    return f"{r.path} #{r.chunk_index}"


def _count_check(
    check: str, bad: int, total: int, threshold: float, rule: str, examples: Sequence[str]
) -> DqResult:
    if total == 0:
        return DqResult(check, FAIL, bad, 0, None, threshold, rule, ("no chunks to check",))
    status = FAIL if bad > threshold * total else PASS
    return DqResult(check, status, bad, total, bad / total, threshold, rule, tuple(examples[:MAX_EXAMPLES]))


def _extra_copies(values: Sequence[str]) -> tuple[int, list[tuple[str, int]]]:
    """Rows beyond the first of each repeated value, and the repeated values with their counts."""
    repeated = sorted(((v, n) for v, n in Counter(values).items() if n > 1), key=lambda x: (-x[1], x[0]))
    return sum(n - 1 for _, n in repeated), repeated


def _metadata_problems(r: SilverRecord, config: Config, tag: str) -> list[str]:
    problems = []
    if not (r.path.startswith(f"{config.docs_dir}/") and r.path.endswith(config.doc_suffix)):
        problems.append("path")
    if not r.section_path or not all(name.strip() for name in r.section_path):
        problems.append("section path")
    if r.version_tag != tag:
        problems.append("version_tag")
    if not _COMMIT.fullmatch(r.commit_sha):
        problems.append("commit_sha")
    return problems


def _drift(
    check: str, to: int, baseline: int | None, kind: str, dq: DqConfig, accept: Acceptance | None
) -> DqResult:
    threshold = dq.max_drift(kind)
    rule = (
        f"{kind} release: |to/from - 1| over {threshold:.0%} fails unless this run accepts the drift of a "
        f"failed run with exactly these numbers; a loss over {dq.max_override_loss:.0%} fails anyway; not "
        "applicable without a baseline"
    )
    if baseline is None:
        return DqResult(check, NOT_APPLICABLE, to, 0, None, threshold, rule)
    if baseline == 0:
        return DqResult(check, FAIL, to, 0, None, threshold, rule, ("the baseline has none",))
    observed = to / baseline - 1
    if abs(to - baseline) <= threshold * baseline:
        return DqResult(check, PASS, to, baseline, observed, threshold, rule)
    within_ceiling = baseline - to <= dq.max_override_loss * baseline
    if accept is not None and within_ceiling and (check, to, baseline) in accept.drift:
        return DqResult(
            check, OVERRIDDEN, to, baseline, observed, threshold, rule, (), accept.reason, accept.run_id
        )
    note: tuple[str, ...] = ()
    if accept is not None:
        note = (
            ("a loss this large cannot be overridden",)
            if not within_ceiling
            else (f"not the drift run {accept.run_id} failed on",)
        )
    return DqResult(check, FAIL, to, baseline, observed, threshold, rule, note)


def evaluate(
    records: Sequence[SilverRecord],
    target_docs: int,
    baseline: Baseline | None,
    config: Config,
    chunking: ChunkConfig,
    tag: str,
    accept: Acceptance | None = None,
) -> list[DqResult]:
    """Every check of CHECKS on the target's chunks (all of them) and its totals, in CHECKS order.

    `target_docs` counts the target's docs, including those with no chunks; `baseline` is None for an
    initial run. Token counts are recounted here rather than read from n_tokens.
    """
    dq = config.dq
    n = len(records)
    rows = sorted(records, key=lambda r: (r.path, r.chunk_index))
    limit = min(chunking.max_tokens, config.embedding_max_seq_length - SPECIAL_TOKENS)

    counted = [(r, count_tokens(r.body)) for r in rows]
    empty = [r for r, k in counted if k == 0 or not r.body.strip() or not r.embed_text.strip()]
    tiny = [(r, k) for r, k in counted if k < dq.tiny_body_tokens]
    oversized = [(r, k) for r in rows if (k := max(count_tokens(r.embed_text), r.n_tokens)) > limit]
    extra_ids, repeated_ids = _extra_copies([r.chunk_id for r in rows])
    extra_texts, repeated_texts = _extra_copies([r.embed_text for r in rows])
    paths_of: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        paths_of[r.embed_text].add(r.path)
    missing = [(r, p) for r in rows if (p := _metadata_problems(r, config, tag))]
    target = Totals(target_docs, n, sum(r.n_tokens for r in rows))
    kind = release_kind(baseline.tag, tag) if baseline else "patch"
    base = baseline.totals if baseline else None

    return [
        _count_check(
            "empty_chunks",
            len(empty),
            n,
            0.0,
            "body with no token (blank, or only characters the tokenizer drops), or blank embed text; "
            "fails if any",
            [f"{_where(r)}: {r.body[:60]!r}" for r in empty],
        ),
        _count_check(
            "tiny_chunks",
            len(tiny),
            n,
            dq.max_tiny_share,
            f"body under {dq.tiny_body_tokens} tokens; fails above {dq.max_tiny_share:.0%} of chunks",
            [f"{_where(r)}: {k} tokens {r.body[:60]!r}" for r, k in tiny],
        ),
        _count_check(
            "oversized_chunks",
            len(oversized),
            n,
            0.0,
            f"embed text over {limit} tokens (max_tokens {chunking.max_tokens}; the embedding models' "
            f"{config.embedding_max_seq_length} less [CLS] and [SEP]); fails if any",
            [f"{_where(r)}: {k} tokens" for r, k in oversized],
        ),
        _count_check(
            "duplicate_chunk_ids",
            extra_ids,
            n,
            0.0,
            "rows beyond the first of a repeated chunk_id; fails if any",
            [f"{i} on {k} rows" for i, k in repeated_ids],
        ),
        _count_check(
            "duplicate_embed_text",
            extra_texts,
            n,
            dq.max_duplicate_embed_share,
            "copies beyond the first of a repeated embed text; fails above "
            f"{dq.max_duplicate_embed_share:.0%} of chunks",
            [f"{k}x in {sorted(paths_of[t])}: {t[:80]!r}" for t, k in repeated_texts],
        ),
        _count_check(
            "missing_metadata",
            len(missing),
            n,
            0.0,
            f"a {config.docs_dir}/*{config.doc_suffix} path, non-blank section names (the title first), "
            f"version_tag {tag}, a 40-hex commit_sha; fails if any row misses one",
            [f"{_where(r)}: {', '.join(p)}" for r, p in missing],
        ),
        _drift("drift_docs", target.docs, base.docs if base else None, kind, dq, accept),
        _drift("drift_chunks", target.chunks, base.chunks if base else None, kind, dq, accept),
        _drift("drift_tokens", target.tokens, base.tokens if base else None, kind, dq, accept),
    ]


def describe(r: DqResult) -> str:
    if r.observed is None:
        return f"{r.check} {r.status}: {r.numerator}/{r.denominator} ({'; '.join(r.examples)})"
    if r.check in DRIFT_CHECKS:
        change = f"{r.denominator} -> {r.numerator} ({r.observed:+.1%}"
        return f"{r.check} {r.status}: {change}, limit {r.threshold:.0%})"
    return f"{r.check} {r.status}: {r.numerator}/{r.denominator} ({r.observed:.2%}, limit {r.threshold:.0%})"


def gate(results: Sequence[DqResult], accept: Acceptance | None) -> GatePassed:
    """GatePassed if every check passed or was overridden as the rules allow; otherwise raise DqFailed."""
    names = [r.check for r in results]
    if sorted(names) != sorted(CHECKS):
        raise DqFailed(f"DQ results are not one per check: {names}")
    allowed = {PASS, FAIL, OVERRIDDEN, NOT_APPLICABLE}
    odd = [
        r.check
        for r in results
        if r.status not in allowed
        or (r.status in (OVERRIDDEN, NOT_APPLICABLE) and r.check not in DRIFT_CHECKS)
        or (r.status == OVERRIDDEN) != (r.override_reason is not None)
        or (r.status == OVERRIDDEN) != (r.override_of is not None)
    ]
    if odd:
        raise DqFailed(f"DQ results with a status their check cannot have: {odd}")
    failed = [r for r in results if r.status == FAIL]
    if failed:
        raise DqFailed("DQ gate failed: " + "; ".join(describe(r) for r in failed))
    overridden = [r for r in results if r.status == OVERRIDDEN]
    if accept is None:
        if overridden:
            raise DqFailed("a check is overridden, but this run accepts no drift")
        return GatePassed(tuple(names), (), None, None, _ISSUED)
    if not overridden:
        raise DqFailed(
            f"--accept-drift-of {accept.run_id} was given, but no drift check is over its threshold: "
            "nothing to accept"
        )
    if {(r.check, r.numerator, r.denominator) for r in overridden} != accept.drift:
        raise DqFailed(f"this run's drift is not the drift run {accept.run_id} failed on")
    if {(r.override_reason, r.override_of) for r in overridden} != {(accept.reason, accept.run_id)}:
        raise DqFailed("an overridden check names another reason or run than this run's acceptance")
    return GatePassed(tuple(names), tuple(r.check for r in overridden), accept.reason, accept.run_id, _ISSUED)


# --- vector checks (D-014): after embedding, before the gold MERGE ----------------------------------------

VECTOR_CHECKS = ("vector_ids", "vector_dimension", "vector_finite", "vector_norm", "embedding_canary")


def vector_checks(
    ids: Sequence[str],
    expected_ids: frozenset[str],
    vectors: Sequence[Sequence[float]],
    dimension: int,
    canary_cosines: Sequence[float],
    dq: DqConfig,
) -> list[DqResult]:
    """Checks on the vectors of one run's added chunks, logged like the chunk checks: one vector per added
    chunk (the ids, not only the count), the model's dimension, finite values, unit norm, and the canary: the
    model must reproduce committed reference vectors, which catches a wrong model or a pooling change that
    every other check would pass."""
    extra = len(set(ids) ^ expected_ids) + (len(ids) - len(set(ids))) + abs(len(ids) - len(vectors))
    wrong_dim = [i for i, v in zip(ids, vectors, strict=False) if len(v) != dimension]
    not_finite = [i for i, v in zip(ids, vectors, strict=False) if not all(math.isfinite(x) for x in v)]
    off_norm = [
        (i, n)
        for i, v in zip(ids, vectors, strict=False)
        if abs((n := math.sqrt(sum(x * x for x in v))) - 1.0) > dq.max_norm_error
    ]
    low = [c for c in canary_cosines if c < dq.min_canary_cosine]
    n = len(expected_ids)
    return [
        _count_check(
            "vector_ids",
            extra,
            n,
            0.0,
            "exactly one vector per added chunk, by chunk_id; fails if any is missing, extra or repeated",
            sorted(set(ids) ^ expected_ids),
        ),
        _count_check(
            "vector_dimension",
            len(wrong_dim),
            n,
            0.0,
            f"a vector not of {dimension} values; fails if any",
            wrong_dim,
        ),
        _count_check(
            "vector_finite", len(not_finite), n, 0.0, "a NaN or infinite value; fails if any", not_finite
        ),
        _count_check(
            "vector_norm",
            len(off_norm),
            n,
            0.0,
            f"L2 norm off 1 by more than {dq.max_norm_error}; fails if any",
            [f"{i}: {v:.6f}" for i, v in off_norm],
        ),
        _count_check(
            "embedding_canary",
            len(low),
            len(canary_cosines),
            0.0,
            f"cosine with the committed reference vector under {dq.min_canary_cosine}; fails if any",
            [f"{c:.6f}" for c in canary_cosines],
        ),
    ]


@dataclass(frozen=True)
class VectorsPassed:
    """Issued only by `gate_vectors`: the run's vectors passed every check (or it embedded nothing)."""

    checked: int
    issued: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.issued is not _ISSUED:
            raise TypeError("a VectorsPassed is issued only by docs_rag.dq.gate_vectors")


def gate_vectors(results: Sequence[DqResult], n_added: int) -> VectorsPassed:
    """VectorsPassed when every vector check passed; a run that adds no chunk embeds nothing and has none."""
    if n_added == 0:
        if results:
            raise DqFailed("vector checks for a run that embedded nothing")
        return VectorsPassed(0, _ISSUED)
    if sorted(r.check for r in results) != sorted(VECTOR_CHECKS):
        raise DqFailed(f"vector results are not one per check: {[r.check for r in results]}")
    failed = [r for r in results if r.status != PASS]
    if failed:
        raise DqFailed("vector checks failed: " + "; ".join(describe(r) for r in failed))
    return VectorsPassed(n_added, _ISSUED)


# --- the ops_dq_results row ---------------------------------------------------------------------------------

DQ_RESULTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING NOT NULL"),
    ("attempt_id", "STRING NOT NULL"),  # one per job process, so one attempt's rows are told from another's
    ("check_name", "STRING NOT NULL"),
    ("status", "STRING NOT NULL"),  # pass | fail | overridden | not_applicable
    ("numerator", "BIGINT NOT NULL"),
    ("denominator", "BIGINT NOT NULL"),
    ("observed", "DOUBLE"),
    ("threshold", "DOUBLE NOT NULL"),
    ("rule", "STRING NOT NULL"),
    ("examples", "ARRAY<STRING>"),
    ("override_reason", "STRING"),
    ("source_repo", "STRING NOT NULL"),
    ("sink", "STRING NOT NULL"),
    ("config_id", "STRING NOT NULL"),
    ("from_version_tag", "STRING"),
    ("to_version_tag", "STRING NOT NULL"),
    ("baseline_run_id", "STRING"),
    ("checked_at", "TIMESTAMP NOT NULL"),
    ("code_version", "STRING NOT NULL"),
    # Added after M4 with ALTER TABLE ADD COLUMNS, so it comes last: the failed run whose drift an overridden
    # check accepted (D-012, owner's decision of 2026-10-20).
    ("override_of_run_id", "STRING"),
)


def dq_results_ddl() -> str:
    return ", ".join(f"{name} {kind}" for name, kind in DQ_RESULTS_COLUMNS)


@dataclass(frozen=True)
class DqRunContext:
    """What every ops_dq_results row of one run attempt shares."""

    run_id: str
    attempt_id: str
    source_repo: str
    sink: str
    config_id: str
    from_version_tag: str | None
    to_version_tag: str
    baseline_run_id: str | None
    checked_at: dt.datetime
    code_version: str


def result_rows(results: Sequence[DqResult], context: DqRunContext) -> list[dict[str, object]]:
    """One ops_dq_results row per result, keyed in DQ_RESULTS_COLUMNS order."""
    return [
        {
            "run_id": context.run_id,
            "attempt_id": context.attempt_id,
            "check_name": r.check,
            "status": r.status,
            "numerator": r.numerator,
            "denominator": r.denominator,
            "observed": r.observed,
            "threshold": r.threshold,
            "rule": r.rule,
            "examples": list(r.examples),
            "override_reason": r.override_reason,
            "source_repo": context.source_repo,
            "sink": context.sink,
            "config_id": context.config_id,
            "from_version_tag": context.from_version_tag,
            "to_version_tag": context.to_version_tag,
            "baseline_run_id": context.baseline_run_id,
            "checked_at": context.checked_at,
            "code_version": context.code_version,
            "override_of_run_id": r.override_of,
        }
        for r in results
    ]
