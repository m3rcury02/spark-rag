# Vendored data

`bert_uncased_vocab.txt`: the BERT uncased WordPiece vocabulary (30,522 entries), as shipped byte-for-byte
in both embedding models' Hugging Face repos:

- `sentence-transformers/all-MiniLM-L6-v2` (Apache-2.0)
- `BAAI/bge-small-en-v1.5` (MIT)

sha256 `07eced375cec144d27c900241f3e339478dec958f92fddbc551f295c992038a3` (checked when loaded, see
`docs_rag/tokens.py`). Downloaded 2026-10-15 from `https://huggingface.co/<model>/resolve/main/vocab.txt`.
