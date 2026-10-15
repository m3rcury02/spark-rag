"""Token counting in the embedding models' own units.

Both embedding models (all-MiniLM-L6-v2 and bge-small-en-v1.5) use the same BERT uncased WordPiece vocabulary
(byte-identical vocab.txt) and the same tokenizer settings, so one count is exact for both. Chunk sizes are
therefore stated in real model tokens; only the models' input limits differ (256 vs 512 tokens including the
[CLS] and [SEP] specials). The vocab is vendored so counting needs no network.
"""

import functools
import hashlib
from importlib import resources

from tokenizers import BertWordPieceTokenizer

VOCAB_FILE = "bert_uncased_vocab.txt"
VOCAB_SHA256 = "07eced375cec144d27c900241f3e339478dec958f92fddbc551f295c992038a3"
SPECIAL_TOKENS = 2  # [CLS] and [SEP], added by the models around every input


@functools.cache
def _tokenizer() -> BertWordPieceTokenizer:
    vocab = resources.files("docs_rag").joinpath("data", VOCAB_FILE)
    digest = hashlib.sha256(vocab.read_bytes()).hexdigest()
    if digest != VOCAB_SHA256:
        raise RuntimeError(f"vendored vocab has sha256 {digest}, expected {VOCAB_SHA256}")
    with resources.as_file(vocab) as path:
        return BertWordPieceTokenizer(
            str(path), lowercase=True, strip_accents=None, clean_text=True, handle_chinese_chars=True
        )


def count_tokens(text: str) -> int:
    """WordPiece tokens in `text`, without the [CLS]/[SEP] specials."""
    return len(_tokenizer().encode(text, add_special_tokens=False).ids)


def token_spans(text: str) -> list[tuple[int, int]]:
    """Character (start, end) of every token in `text`, for cutting text at token boundaries."""
    return [(start, end) for start, end in _tokenizer().encode(text, add_special_tokens=False).offsets]
