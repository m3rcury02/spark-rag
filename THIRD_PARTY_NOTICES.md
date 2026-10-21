# Third-party material in this repository

The project's own code is licensed under the Apache License 2.0 (`LICENSE`). This repository also
redistributes the following third-party files. Their licenses apply to those files only.
The full Apache License 2.0 text is in `LICENSES/Apache-2.0.txt` (copied from
https://www.apache.org/licenses/LICENSE-2.0.txt on 2026-10-15).

## BERT uncased WordPiece vocabulary

- File: `src/docs_rag/data/bert_uncased_vocab.txt` (unmodified; sha256 `07eced37…`, see `src/docs_rag/data/README.md`).
- Origin: Google's BERT release (google-research/bert, Apache License 2.0). Taken byte-for-byte from the
  Hugging Face repo `sentence-transformers/all-MiniLM-L6-v2` (Apache License 2.0), and redistributed under
  that license. The same file ships in `BAAI/bge-small-en-v1.5` (MIT).

## Apache Spark documentation excerpts

- Files: test fixtures under `tests/` that quote short passages of `docs/*.md` from apache/spark, some of
  them shortened or edited to isolate one case; the retrieval eval under `eval/` (section texts of v4.0.1 and
  verbatim quotes); and the three canary texts in `src/docs_rag/data/embedding_canary.json`.
- License: Apache License 2.0. Attribution from Apache Spark's NOTICE file:

  > Apache Spark
  > Copyright 2014 and onwards The Apache Software Foundation.
  >
  > This product includes software developed at
  > The Apache Software Foundation (http://www.apache.org/).
