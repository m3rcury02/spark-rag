# Spark Docs RAG Pipeline

An incremental document pipeline that keeps a vector index in sync with a changing corpus (the
Apache Spark docs across release tags), plus a retrieval evaluation harness.

**Status: work in progress.** M0 (platform research and scaffold) and M1 (bronze ingestion) are done. See `CLAUDE.md` for the
milestone tracker and `DECISIONS.md` for design decisions.

No results are reported yet. Every number that appears here later comes from a run whose raw output
is committed under `runs/`.

Corpus: Markdown files under `docs/` in [apache/spark](https://github.com/apache/spark), Apache License 2.0.
