"""Typed pipeline configuration: one object passed into every step, so nothing is hard-coded in functions."""

import re
from dataclasses import dataclass

from docs_rag.chunk import ChunkConfig
from docs_rag.tokens import SPECIAL_TOKENS

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_TAG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _check(pattern: re.Pattern[str], value: str, what: str) -> str:
    if not pattern.fullmatch(value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


def validate_tag(tag: str) -> str:
    """A git tag name we accept, e.g. 'v4.0.1'. Tags end up in URLs and SQL parameters."""
    return _check(_TAG, tag, "tag")


@dataclass(frozen=True)
class DqConfig:
    """Thresholds of the DQ gate between silver and gold (D-012). Shares are of all the target's chunks; drift
    is to/from - 1 of the target's docs, chunks and tokens against the baseline run's, with a threshold by
    release kind (docs_rag.changes.release_kind; the owner's choice of 2026-10-20)."""

    tiny_body_tokens: int = 5  # a chunk whose body has fewer WordPiece tokens is tiny
    max_tiny_share: float = 0.02
    max_duplicate_embed_share: float = 0.01  # extra copies of an embed text, over all chunks
    max_drift_patch: float = 0.05  # a patch release, the same tag, or tags that are not releases
    max_drift_feature: float = 0.10  # a minor or major release
    max_override_loss: float = 0.50  # a loss beyond this fails even when the drift is accepted
    min_reason_chars: int = 20  # an --accept-drift reason has at least this many characters and 4 words
    max_norm_error: float = 1e-3  # a stored vector's L2 norm must be within this of 1 (they are normalized)
    min_canary_cosine: float = 0.999  # the model must reproduce the committed canary vectors (D-014)

    def __post_init__(self) -> None:
        if self.tiny_body_tokens < 1 or self.min_reason_chars < 1:
            raise ValueError("tiny_body_tokens and min_reason_chars must be positive")
        drift = (self.max_drift_patch, self.max_drift_feature)
        shares = (self.max_tiny_share, self.max_duplicate_embed_share, *drift, self.max_override_loss)
        if not all(0 <= s < 1 for s in shares) or self.max_override_loss < max(drift):
            raise ValueError(f"invalid DQ thresholds: {self}")

    def max_drift(self, kind: str) -> float:
        """The drift threshold for a release kind; anything but 'minor' or 'major' gets the strictest."""
        return self.max_drift_feature if kind in ("minor", "major") else self.max_drift_patch


@dataclass(frozen=True)
class EmbeddingConfig:
    """The model of the published index (D-014): a free sentence-transformers model pinned to a Hugging Face
    revision (the one M2's tokenizer parity run checked, runs/m2_f), vectors normalized so that the index's L2
    ranking orders like cosine similarity."""

    model: str = "sentence-transformers/all-MiniLM-L6-v2"
    revision: str = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
    dimension: int = 384
    batch_size: int = 64
    # Installed by the jobs that embed (scripts/run_job.py) and checked at their start (docs_rag.jobenv): a
    # different torch or transformers can change the vectors. torch is the CPU build (no CUDA libraries).
    packages: tuple[tuple[str, str], ...] = (
        ("torch", "2.14.1+cpu"),
        ("sentence-transformers", "6.1.0"),
        ("transformers", "5.19.0"),
    )

    # Prepended to a query (never to a document) before it is embedded: what the model's card asks for
    # short-query-to-passage retrieval. all-MiniLM-L6-v2 takes none.
    query_prefix: str = ""

    @property
    def model_id(self) -> str:
        """What every gold row and run-log row records: the model and its exact revision."""
        return f"{self.model}@{self.revision}"


BGE_SMALL = EmbeddingConfig(
    model="BAAI/bge-small-en-v1.5",
    revision="5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",  # the one M2's parity run checked (runs/m2_f)
    # From the model card, for short queries against passages; its sentence-transformers config sets no
    # prompt (runs/m2_f), so it is added here. M6 also measures it without (D-015).
    query_prefix="Represent this sentence for searching relevant passages: ",
)


@dataclass(frozen=True)
class EvalConfig:
    """The retrieval eval (M6, D-015), fixed before the first run: the metrics' cutoffs, the bootstrap, and
    what makes a quote label valid and a chunk relevant (docs_rag.evaluate)."""

    recall_k: int = 5
    mrr_k: int = 10
    resamples: int = 10_000
    seed: int = 15
    min_quote_share: float = 0.5  # a chunk is relevant if it covers this share of an occurrence of the quote
    quote_chars: tuple[int, int] = (30, 250)
    models: tuple[EmbeddingConfig, ...] = (EmbeddingConfig(), BGE_SMALL)

    def __post_init__(self) -> None:
        if not (0 < self.min_quote_share <= 1 and 0 < self.recall_k <= self.mrr_k and self.resamples > 0):
            raise ValueError(f"invalid eval config: {self}")


@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"
    schema: str = "docs_rag_dev"
    source_repo: str = "apache/spark"
    docs_dir: str = "docs"
    doc_suffix: str = ".md"
    chunk: ChunkConfig = ChunkConfig()  # what gets embedded and published (M5)
    # The chunk configs M6 compares (D-009): 254 and 128 tokens, both within MiniLM's 256-token limit, with
    # overlap at about an eighth of the budget, so the two differ in size only. Silver holds both.
    chunk_grid: tuple[ChunkConfig, ...] = (ChunkConfig(254, 32), ChunkConfig(128, 16))
    # Docs that legitimately give no chunks: only headings plus generated includes (D-008). Any other doc
    # with no chunks fails the silver run, so a cleaning bug that empties pages cannot pass silently.
    zero_chunk_docs: frozenset[str] = frozenset({"docs/sql-ref-functions-builtin.md"})
    # What a committed pipeline run publishes to; the baseline chain in ops_run_log is per sink (D-011): the
    # gold table (D-014), or 'none', the chain of M3/M4, which ends at the gate and publishes nothing.
    sink: str = "gold_chunk_embeddings"
    embedding: EmbeddingConfig = EmbeddingConfig()
    endpoint: str = "docs_rag_endpoint"  # the one AI Search endpoint Free Edition allows (D-001)
    index_name: str = "gold_chunk_index"  # Delta Sync index over the gold table
    # The smaller input limit of the two embedding models (all-MiniLM-L6-v2 256, bge-small-en-v1.5 512), with
    # [CLS] and [SEP] included: every chunk config must fit it, so no chunk is truncated by the model.
    embedding_max_seq_length: int = 256
    dq: DqConfig = DqConfig()
    eval: EvalConfig = EvalConfig()

    def __post_init__(self) -> None:
        _check(_IDENTIFIER, self.catalog, "catalog")
        _check(_IDENTIFIER, self.schema, "schema")
        _check(_REPO, self.source_repo, "source repo")
        _check(_IDENTIFIER, self.docs_dir, "docs dir")
        if not self.doc_suffix.startswith("."):
            raise ValueError(f"invalid doc suffix: {self.doc_suffix!r}")
        if self.sink != "none":
            _check(_IDENTIFIER, self.sink, "sink")
        _check(_IDENTIFIER, self.index_name, "index name")
        for c in (self.chunk, *self.chunk_grid):
            if c.max_tokens + SPECIAL_TOKENS > self.embedding_max_seq_length:
                raise ValueError(
                    f"chunk config {c.max_tokens}/{c.overlap_tokens} does not fit the embedding models' "
                    f"{self.embedding_max_seq_length} tokens with [CLS] and [SEP]"
                )

    def table(self, name: str) -> str:
        """Fully qualified Unity Catalog name, e.g. 'workspace.docs_rag_dev.bronze_docs'."""
        return f"{self.catalog}.{self.schema}.{_check(_IDENTIFIER, name, 'table name')}"

    @property
    def bronze_table(self) -> str:
        return self.table("bronze_docs")

    @property
    def silver_table(self) -> str:
        return self.table("silver_chunks")

    @property
    def run_log_table(self) -> str:
        return self.table("ops_run_log")

    @property
    def dq_results_table(self) -> str:
        return self.table("ops_dq_results")

    @property
    def gold_table(self) -> str | None:
        """The table a committed run publishes to, or None for the sink 'none'."""
        return None if self.sink == "none" else self.table(self.sink)

    @property
    def index(self) -> str:
        return self.table(self.index_name)

    def volume(self, name: str) -> str:
        """Path of a Unity Catalog Volume in this schema, e.g. '/Volumes/workspace/docs_rag_dev/artifacts'."""
        return f"/Volumes/{self.catalog}/{self.schema}/{_check(_IDENTIFIER, name, 'volume name')}"
