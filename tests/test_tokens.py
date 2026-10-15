from docs_rag.tokens import count_tokens, token_spans


def test_counts_wordpieces_lowercased_without_specials() -> None:
    # "spark" and "streaming" are whole words in the BERT uncased vocab; "Streaming" is lowercased first.
    assert count_tokens("Spark Streaming") == 2


def test_unknown_words_split_into_wordpieces() -> None:
    assert count_tokens("spark.sql.shuffle.partitions") > 4  # dots are tokens; words may split further


def test_empty_text_has_no_tokens() -> None:
    assert count_tokens("") == 0
    assert token_spans("") == []


def test_spans_cover_the_tokens_in_order() -> None:
    text = "Set spark.executor.memory"
    spans = token_spans(text)
    assert len(spans) == count_tokens(text)
    assert spans == sorted(spans)
    assert text[spans[0][0] : spans[0][1]] == "Set"
