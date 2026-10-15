from pathlib import Path

import pytest

from docs_rag.clean import clean_inline, clean_page, fenced_blocks, residue, split_front_matter

FIXTURE = Path(__file__).parent / "fixtures" / "spark_page.md"


def clean(body: str) -> str:
    return clean_page(body, fallback_title="fallback").text


# --- front matter and title ---


def test_title_prefers_display_title_and_skips_block_scalars() -> None:
    meta, body = split_front_matter(
        "---\ntitle: T\ndisplayTitle: Shown\nlicense: |\n  long text\n---\nbody\n"
    )
    assert meta == {"title": "T", "displayTitle": "Shown"}
    assert body == "body\n"


def test_title_falls_back_to_the_file_name() -> None:
    assert clean_page("no front matter", fallback_title="quick-start").title == "quick-start"


def test_fixture_page_cleans_without_residue() -> None:
    doc = clean_page(FIXTURE.read_text(), fallback_title="x")
    assert doc.title == "Quick Start"
    assert residue(doc.text) == {"liquid_tags": 0, "html_tags": 0, "markdown_links": 0}
    assert "This div is only here" not in doc.text  # d-none block dropped
    assert "table of contents" not in doc.text  # the {:toc} bullet dropped
    assert "{:toc}" not in doc.text


# --- Liquid ---


def test_highlight_becomes_a_fenced_block() -> None:
    assert clean("{% highlight scala %}\nval x = 1\n{% endhighlight %}") == "```scala\nval x = 1\n```"


def test_include_example_becomes_a_pointer_and_generated_includes_are_dropped() -> None:
    text = clean("A\n\n{% include_example python/ml/tf_idf_example.py %}\n\n{% include_api_gen gen.html %}\n")
    assert text == "A\n\n[example code: python/ml/tf_idf_example.py]"


def test_raw_keeps_content_and_comment_drops_it() -> None:
    assert clean("{% raw %}{{ x }}{% endraw %} {% comment %}hidden{% endcomment %}kept") == "{{ x }} kept"


def test_site_variables_are_left_literal() -> None:
    # Substituting the release version would change the text, and so the chunk IDs, of unchanged pages.
    assert clean("Spark {{site.SPARK_VERSION}} docs") == "Spark {{site.SPARK_VERSION}} docs"


def test_unknown_liquid_tag_raises() -> None:
    with pytest.raises(ValueError, match="unknown Liquid tag"):
        clean("{% assign x = 1 %}")


def test_unclosed_raw_raises() -> None:
    with pytest.raises(ValueError, match="unclosed"):
        clean("{% raw %} never closed")


# --- code is protected from HTML and Markdown cleaning ---


def test_angle_brackets_in_code_survive() -> None:
    text = (
        "Use `Dataset<Row>` here.\n\n```xml\n<property><name>a</name></property>\n```\n\n"
        "<pre>ssh <driver-node></pre>"
    )
    out = clean(text)
    assert "`Dataset<Row>`" in out
    assert "<property><name>a</name></property>" in out
    assert "ssh <driver-node>" in out


def test_markdown_inside_code_is_not_touched() -> None:
    assert clean("```\nsee [a](b) and {:toc}\n```") == "```\nsee [a](b) and {:toc}\n```"


def test_fences_follow_commonmark() -> None:
    lines = [
        "> ```scala",  # a fence inside a blockquote
        "> val x = 1",
        "> ```",
        "Wrap ```code``` in a sentence",  # inline code, not a fence: the info string holds backticks
        "~~~",
        "```",  # a backtick line does not close a tilde fence
        "~~~",
    ]
    assert fenced_blocks(lines) == [(0, 2), (4, 6)]


def test_unclosed_fence_raises() -> None:
    with pytest.raises(ValueError, match="unclosed code fence"):
        clean("```python\nprint(1)\n")


# --- HTML ---


def test_table_becomes_one_line_per_row() -> None:
    table = (
        "<table><thead><tr><th>Property Name</th><th>Default</th></tr></thead>"
        "<tr><td><code>spark.app.name</code></td><td>(none)</td></tr></table>"
    )
    assert clean(table) == "Property Name | Default\n`spark.app.name` | (none)"


def test_code_block_in_a_table_cell_is_lifted_out_of_the_row() -> None:
    table = "<table><tr><td>Example</td><td>\n\n```sql\nSELECT 1;\n```\n\n</td></tr></table>"
    assert clean(table) == "Example\n\n```sql\nSELECT 1;\n```"


def test_language_tabs_get_a_label_line() -> None:
    out = clean('<div class="codetabs">\n<div data-lang="python" markdown="1">\nA\n</div>\n</div>')
    assert out == "python:\n\nA"


def test_only_allowlisted_tags_are_stripped_and_entities_unescaped() -> None:
    assert clean("<b>bold</b> &lt;b&gt; <driver-node> a &amp; b") == "bold <b> <driver-node> a & b"


# --- Markdown ---


def test_links_keep_their_text() -> None:
    text = "[guide](a.html), [[SPARK-1]](https://x), [ref link][r], [wrapped\ntext](u)\n\n[r]: https://x"
    assert clean(text) == "guide, [SPARK-1], ref link, wrapped\ntext"


def test_images_and_html_comments_are_removed() -> None:
    assert clean("A ![diagram](img/x.png) B <!-- note -->C") == "A  B C"


def test_link_text_does_not_cross_a_blank_line() -> None:
    assert clean("[not a link\n\ntext](u)") == "[not a link\n\ntext](u)"


def test_clean_inline_drops_header_ids_and_markup() -> None:
    assert clean_inline("Using [`spark-submit`](x.html) {#launching}") == "Using `spark-submit`"


def test_residue_counts_markup_outside_code_only() -> None:
    assert residue("{% x %} <div> [a](b) `<div>`\n```\n[c](d) <div>\n```") == {
        "liquid_tags": 1,
        "html_tags": 1,
        "markdown_links": 1,
    }


def test_long_table_borders_are_shortened_inside_code_too() -> None:
    text = "```\n+--------+-----+\n| a      | b   |\n+--------+-----+\n```\n\n|:-------|=====|\n\n---"
    assert clean(text) == "```\n+---+---+\n| a      | b   |\n+---+---+\n```\n\n|:---|===|\n\n---"


def test_text_lines_with_dashes_are_left_alone() -> None:
    assert clean("```\n# ---------- section ----------\nx = a ---- b\n```") == (
        "```\n# ---------- section ----------\nx = a ---- b\n```"
    )


def test_mathjax_macro_preamble_is_dropped() -> None:
    preamble = "`\\[\n\\newcommand{\\R}{\\mathbb{R}}\n\\newcommand{\\x}{\\mathbf{x}}\n\\]`"
    page = f"Intro.\n\n{preamble}\n\nBody `$\\R$`."
    assert clean(page) == "Intro.\n\nBody `$\\R$`."
