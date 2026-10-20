from pathlib import Path

import pytest

import docs_rag.clean
from docs_rag.clean import clean_inline, clean_page, fenced_blocks, split_front_matter

FIXTURE = Path(__file__).parent / "fixtures" / "spark_page.md"
NO_RESIDUE = {"liquid_tags": 0, "html_tags": 0, "markdown_links": 0, "broken_table_headers": 0}


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
    assert doc.residue == NO_RESIDUE
    assert "This div is only here" not in doc.text  # d-none block dropped
    assert "table of contents" not in doc.text  # the {:toc} bullet dropped
    assert "{:toc}" not in doc.text


# --- Liquid ---


def test_highlight_becomes_a_fenced_block() -> None:
    assert clean("{% highlight scala %}\nval x = 1\n{% endhighlight %}") == "```scala\nval x = 1\n```"


def test_include_example_becomes_a_short_pointer_and_generated_includes_are_dropped() -> None:
    text = clean("A\n\n{% include_example python/ml/tf_idf_example.py %}\n\n{% include_api_gen gen.html %}\n")
    assert text == "A\n\n[example: tf_idf_example.py]"


def test_include_example_keeps_its_snippet_label() -> None:
    assert (
        clean("{% include_example csv_dataset python/sql/datasource.py %}")
        == "[example: datasource.py, csv_dataset]"
    )


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


def test_fence_rules_on_real_corpus_lines() -> None:
    """Lines from structured-streaming-kafka-integration.md and sql-migration-guide.md (v3.5.0-v4.0.1) that
    start with triple backticks but are inline code; a naive toggle swapped code and prose after them."""
    lines = [
        "```kafka.partitioner.class``` option. If not present, Kafka default partitioner",
        "    ```SELECT col FROM (SELECT a + b AS col FROM t1) t2```",
        "## Deploying",
        "```python",
        "# Write key-value data from a DataFrame to Kafka using a topic specified in the data",
        "```",
    ]
    assert fenced_blocks(lines) == [(3, 5)]


def test_unclosed_fence_raises() -> None:
    with pytest.raises(ValueError, match="unclosed code fence"):
        clean("```python\nprint(1)\n")


# --- HTML ---


def test_table_becomes_a_gfm_table_with_one_line_per_row() -> None:
    table = (
        "<table><thead><tr><th>Property Name</th><th>Default</th></tr></thead>"
        "<tr><td><code>spark.app.name</code></td><td>(none)</td></tr></table>"
    )
    assert clean(table) == "Property Name | Default\n--- | ---\n`spark.app.name` | (none)"


def test_table_without_a_header_row_gets_no_delimiter() -> None:
    table = "<table><tr><th>Feature</th><td>yes</td></tr><tr><th>Other</th><td>no</td></tr></table>"
    assert clean(table) == "Feature | yes\nOther | no"


def test_language_tabs_get_a_label_line() -> None:
    out = clean('<div class="codetabs">\n<div data-lang="python" markdown="1">\nA\n</div>\n</div>')
    assert out == "python:\n\nA"


def test_a_tab_that_opens_with_code_fenced_in_its_language_gets_no_label() -> None:
    tab = (
        '<div data-lang="{lang}" markdown="1">\n\n'
        "{{% highlight scala %}}\nval x = 1\n{{% endhighlight %}}\n</div>"
    )
    assert clean(tab.format(lang="scala")) == "```scala\nval x = 1\n```"
    assert clean(tab.format(lang="Scala")) == "```scala\nval x = 1\n```"
    assert clean(tab.format(lang="java")).startswith("java:\n\n```scala")  # another language: kept
    fenced = '<div data-lang="r" markdown="1">\n```r\nx <- 1\n```\n</div>'
    assert clean(fenced) == "```r\nx <- 1\n```"
    rust = '<div data-lang="r" markdown="1">\n```rust\nlet x = 1;\n```\n</div>'
    assert clean(rust).startswith("r:\n\n```rust")


def test_only_allowlisted_tags_are_stripped_and_entities_unescaped() -> None:
    assert clean("<b>bold</b> &lt;b&gt; <driver-node> a &amp; b") == "bold <b> <driver-node> a & b"


# --- Markdown ---


def test_links_keep_their_text() -> None:
    text = "[guide](a.html), [[SPARK-1]](https://x), [ref link][r], [wrapped\ntext](u)\n\n[r]: https://x"
    assert clean(text) == "guide, [SPARK-1], ref link, wrapped\ntext"


def test_link_urls_may_contain_parentheses() -> None:
    text = "[Lasso](http://en.wikipedia.org/wiki/Lasso_(statistics)) and [f](api/D.html#f():D[T]) end"
    assert clean(text) == "Lasso and f end"


def test_images_and_html_comments_are_removed() -> None:
    assert clean("A ![diagram](img/x.png) B <!-- note -->C") == "A  B C"


def test_link_text_does_not_cross_a_blank_line() -> None:
    assert clean("[not a link\n\ntext](u)") == "[not a link\n\ntext](u)"


def test_clean_inline_drops_header_ids_and_markup() -> None:
    assert clean_inline("Using [`spark-submit`](x.html) {#launching}") == "Using `spark-submit`"


def test_residue_counts_markup_left_outside_code() -> None:
    page = (
        "{% raw %}{% x %}{% endraw %} <iframe src=x></iframe> &lt;b&gt; `<div>`\n\n[a\n\nb](c)\n\n"
        "|a|\nb|\n|---|\n\n```\n[c](d) <div>\n```"
    )
    assert clean_page(page, "x").residue == {
        "liquid_tags": 1,  # kept literal by {% raw %}
        "html_tags": 2,  # <iframe> and </iframe>; the escaped &lt;b&gt; is prose, not a tag
        "markdown_links": 1,  # 'b](c)': not a link, since its text crosses a blank line
        "broken_table_headers": 1,  # the header is broken over two lines
    }


def test_cleaning_raises_if_a_rule_swallows_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(docs_rag.clean, "_markdown", lambda text: text.split("\x01")[0])
    with pytest.raises(ValueError, match="cleaning lost"):
        clean_page("Intro.\n\n```\ncode\n```\n", "x")


def test_code_inside_an_html_comment_may_go() -> None:
    assert clean_page("A <!-- `x` -->B", "x").text == "A B"


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


# --- tables: real-page structures ---


def test_a_layout_table_without_rows_keeps_its_cells_as_content() -> None:
    page = (
        '<table width="100%"><td>\n{% highlight bash %}\n$ nc -lk 9999\n{% endhighlight %}\n</td>\n'
        "<td>Two</td></table>"
    )
    assert clean(page) == "```bash\n$ nc -lk 9999\n```\n\nTwo"


def test_rowspan_cells_are_repeated_and_colspan_keeps_columns() -> None:
    page = (
        "<table><tr><th>Left</th><th>Right</th><th>Type</th></tr>"
        '<tr><td rowspan="2">Stream</td><td>Static</td><td>Inner</td></tr>'
        "<tr><td>Stream</td><td>Outer</td></tr>"
        '<tr><td colspan="2">Other</td><td>Any</td></tr></table>'
    )
    assert clean(page) == (
        "Left | Right | Type\n--- | --- | ---\nStream | Static | Inner\nStream | Stream | Outer\n"
        "Other |  | Any"
    )


def test_a_nested_table_is_written_inline_in_its_cell() -> None:
    page = (
        "<table><tr><th>Spark</th><th>Postgres</th></tr><tr><td>ArrayType</td><td><table>"
        "<tr><th>Element</th><th>PG</th></tr><tr><td>IntegerType</td><td>integer[]</td></tr>"
        "</table></td></tr></table>"
    )
    assert clean(page) == "Spark | Postgres\n--- | ---\nArrayType | Element, PG; IntegerType, integer[]"


def test_list_items_in_a_cell_are_joined() -> None:
    page = "<table><tr><td>A</td><td><ul><li>One.</li><li>Two.</li></ul></td></tr></table>"
    assert clean(page) == "A | One.; Two."


def test_one_line_code_in_a_cell_stays_inline_and_longer_code_follows_the_table() -> None:
    page = (
        "<table><tr><th>Name</th><th>Meaning</th></tr>"
        "<tr><td>a</td><td>For example <code><xmp><book></book></xmp></code> here.</td></tr>"
        "<tr><td>b</td><td>\n{% highlight r %}\nf(x,\n  y)\n{% endhighlight %}\n</td></tr>"
        "<tr><td>c</td><td>last</td></tr></table>"
    )
    assert clean(page) == (
        "Name | Meaning\n--- | ---\na | For example `<book></book>` here.\n"
        "b | (code below the table)\nc | last\n\n```r\nf(x,\n  y)\n```"
    )


def test_br_in_a_pipe_table_row_does_not_start_a_new_row() -> None:
    assert clean("|Keyword|Spark SQL<br/>ANSI Mode|\n|--|---|\n|ADD|non-reserved|") == (
        "|Keyword|Spark SQL ANSI Mode|\n|--|---|\n|ADD|non-reserved|"
    )


# --- other real-page constructs ---


def test_footnotes_become_notes_and_their_markers_go() -> None:
    page = "Uses natives[^1].\n\n[^1]: To learn more, you may wish to\n    watch the [talk](http://x.io/)."
    assert clean(page) == "Uses natives.\n\nNote: To learn more, you may wish to watch the talk."


def test_star_and_underscore_rules_are_dropped() -> None:
    assert clean("A\n\n" + "*" * 99 + "\n\n___\n\nB") == "A\n\nB"


def test_style_blocks_go_and_presentational_tags_are_stripped() -> None:
    page = '<style type="text/css">.t {color: red;}</style>**<u>TABLE: person</u>** <kbd>Ctrl</kbd>'
    assert clean(page) == "**TABLE: person** Ctrl"


def test_links_with_titles_wrapped_urls_and_no_closing_paren() -> None:
    page = (
        '![Table](img/t.png "Table") [Guide](https://spark.apache\n.org/docs/x.html) and '
        "[Blog](https://a.b/c/"
    )
    assert clean(page) == "Guide and Blog"


def test_link_title_forms_and_angle_bracket_urls() -> None:
    assert clean("[a](u 't') [b](u (t)) [c](<u v>) [d](u \"t\") end") == "a b c d end"


def test_unknown_tag_like_words_are_kept_and_block_tags_do_not_glue_words() -> None:
    assert clean("Set <col-name> and <s3-bucket> or <q>") == "Set <col-name> and <s3-bucket> or <q>"
    assert clean("one<p>two</p><div>three</div>four") == "one\n\ntwo\n\nthree\n\nfour"


def test_a_commented_out_table_does_not_nest_the_real_one() -> None:
    page = (
        "<!-- <table><tr><td>x</td></tr></table> -->\n<table><tr><th>A</th></tr><tr><td>`b`</td></tr></table>"
    )
    assert clean(page) == "| A |\n| --- |\n| `b` |"  # one column keeps its pipes


def test_code_in_dropped_markup_does_not_trip_the_conservation_check() -> None:
    assert clean("<script>var x = `y`;</script>A") == "A"
    assert clean("![the `code` alt](x.png) B") == "B"


def test_markup_inside_code_spans_is_code_and_escaped_markup_is_prose() -> None:
    assert clean("Use `<pre>` here.\n\n```\nx\n```") == "Use `<pre>` here.\n\n```\nx\n```"
    page = "Write &lt;!-- in HTML, then `code` and more --&gt; end"
    assert clean(page) == "Write <!-- in HTML, then `code` and more --> end"


def test_whitespace_entities_in_cells_become_spaces() -> None:
    page = "<table><tr><th>A&nbsp;B</th><th>C</th></tr><tr><td>&nbsp;x&#160;y</td><td>z</td></tr></table>"
    assert clean(page) == "A B | C\n--- | ---\nx y | z"


@pytest.mark.parametrize("opener", ["## Head", "```\nx\n```"])
def test_a_table_directly_under_a_heading_or_fence_is_not_broken(opener: str) -> None:
    page = f"{opener}\n| a | b |\n| --- | --- |\n| 1 | 2 |"
    assert clean_page(page, "x").residue == NO_RESIDUE
