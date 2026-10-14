# Project brief

The owner's original requirements, kept so every session works from the same spec.
Lightly edited: personal details removed. Where `DECISIONS.md` refines something here
(milestone order, tag choice), the decision log wins.

**Goal:** a portfolio project that shows I can build data pipelines for AI retrieval (RAG).

## Project

An incremental document pipeline that keeps a vector index in sync with a changing document
corpus, plus a retrieval evaluation harness.

Corpus: the Apache Spark documentation (the Markdown files under `docs/` in the apache/spark
GitHub repo, Apache 2.0 licensed). Ingest one release tag first, then a later tag, to prove that
only changed content gets reprocessed.

## What it must do

1. **Ingestion (bronze):** load raw docs with source path, version tag, content hash and ingest
   time. Re-running must never create duplicates.
2. **Parsing and chunking (silver):** clean the Markdown and split it into chunks with stable chunk
   IDs, keeping metadata (doc path, section heading, version). Chunk size and overlap must be
   configurable.
3. **Change detection:** on a new version, detect added, changed and deleted documents by hash.
   Re-embed only changed chunks and remove chunks for deleted docs. Log the counts for every run.
4. **Embeddings and index (gold):** generate embeddings with a free open-source model (e.g.
   sentence-transformers) and load them into a vector index. Prefer Databricks Vector Search
   (now AI Search) synced from a Delta table if the workspace supports it; otherwise use
   Postgres + pgvector in Docker. Check current docs and the workspace's limits before choosing.
5. **Data-quality checks:** empty or tiny chunks, oversized chunks, duplicate chunks, missing
   metadata, and row-count drift between runs. A failed check stops the pipeline.
6. **Retrieval evaluation:** build a test set of 50 question-answer pairs grounded in specific doc
   sections (Claude drafts, the owner reviews). Measure recall@5 and MRR. Compare at least 2 chunk
   sizes and 2 embedding models, and save the results to a table.
7. **Orchestration and deployment:** Databricks Jobs (or a simple scheduler for the local
   fallback), deployed with Databricks Asset Bundles with dev and prod targets, plus a GitHub
   Actions workflow that runs the tests.
8. **Optional, last:** a small FastAPI question-answering endpoint that retrieves chunks, calls an
   LLM, and cites the source chunks in its answers.

## Constraints

- Budget is zero or close to it: Databricks Free Edition (plus an Azure free trial). Flag anything
  that costs money before using it.
- Python with type hints. Pytest tests for chunking, hashing and change detection.
- Small and finished beats big and half-done.

## How to work with the owner

- Keep `CLAUDE.md` (conventions) and `DECISIONS.md` (every significant choice: options considered,
  what was chosen, and why) up to date.
- After each milestone: show how to run it, say what to measure, and give 5 interview-style
  questions about what was just built. Don't start the next milestone until the owner says so.
- Never invent metrics. Every number in the README must come from a run we actually did.
- Final milestone: a README with a Mermaid architecture diagram, how to run it, the experiment
  results table, and known limitations.
- Never sign or co-sign the git commits.
