r"""Turn one Spark docs page (Jekyll Markdown with Liquid and HTML) into clean Markdown text.

The order matters and is fixed (D-008):
1. split off the YAML front matter; the title comes from `displayTitle` (else `title`);
2. Liquid, from an explicit table: `highlight` blocks become fenced code, `include_example` becomes a short
   pointer (`[example: <file name>]`), generated includes and control flow are dropped, `raw` markers are
   dropped with their content kept, `comment` blocks are dropped. An unknown Liquid tag raises: guessing would
   put junk in chunk text;
3. hidden `<div class="d-none">` blocks are dropped with their content (they only hold markup for viewers);
4. code (fenced blocks, `<pre>`/`<xmp>` blocks, inline code spans) is masked, so nothing below touches it.
   Code is full of angle brackets that are not HTML (`Dataset<Row>`, XML config, `<driver-node>`);
5. outside code: HTML tables become GFM tables (one ` | `-joined line per row, a `--- | ---` delimiter after a
   header row), language tabs get a `<lang>:` label line (none when the tab opens with a code block fenced in
   that language: the label would repeat the fence; CLEANER_VERSION 2), only an allowlist of real HTML tag
   names is stripped (text kept), then entities are unescaped;
6. Markdown links keep their text, images and reference-link definitions go, kramdown `{:...}` lines and the
   table-of-contents bullet before `{:toc}` go, HTML comments go;
7. code is restored and whitespace normalised;
8. on lines made only of table-border characters (`+---+---+`, `|:---|---|`), runs of 4 or more `-` or `=`
   are shortened to 3, inside code too: BERT makes every `-` a token, so long borders filled chunks.

A MathJax macro preamble (a `` `\[ \newcommand... \]` `` span holding only macro definitions) is dropped
before step 4: it carries no content and repeats verbatim on several MLlib pages.

`{{ site.X }}` variables are left literal on purpose: substituting the tag's version would make an unchanged
page produce different text in every tag and change its chunk IDs (M3, M5).
"""

import html
import re
from dataclasses import dataclass

# Bump when cleaned text changes for the same input. The goldens (tests/test_clean_golden.py on fixtures,
# scripts/corpus_fingerprint.py on the real corpus) fail on any change and are never overwritten. Chunk
# offsets (char_start/char_end) point into the cleaned text, so this version names their coordinate system.
CLEANER_VERSION = "2"  # 2: no tab label before a code block fenced in the same language (D-012)

# --- 1. front matter -----------------------------------------------------------------------------------

_FRONT_MATTER = re.compile(r"\A---\n(.*?)\n---[ \t]*(?:\n|\Z)", re.S)


def split_front_matter(text: str) -> tuple[dict[str, str], str]:
    """Top-level `key: value` pairs of the YAML front matter (block scalars are skipped), and the body."""
    m = _FRONT_MATTER.match(text)
    if not m:
        return {}, text
    meta: dict[str, str] = {}
    for line in m.group(1).splitlines():
        km = re.match(r"([A-Za-z_][\w-]*):(?:\s+(.*))?$", line)
        if km and (km.group(2) or "").strip() not in ("|", ">", "|-", ">-"):
            meta[km.group(1)] = (km.group(2) or "").strip().strip("\"'")
    return meta, text[m.end() :]


# --- 2. Liquid ---------------------------------------------------------------------------------------------

_LIQUID = re.compile(r"\{%-?\s*(\w+)([^%]*?)-?%\}")
_DROP_TAGS = {"include_api_gen", "include_relative", "for", "endfor", "if", "elsif", "else", "endif", "break"}


def _example_pointer(args: str) -> str:
    """`{% include_example [label] path %}` -> `[example: <file name>]` or `[example: <file name>, <label>]`.

    The code itself lives under examples/ in the source archive, which bronze does not load (D-008)."""
    parts = args.split()
    if not parts:
        return ""
    name = parts[-1].rsplit("/", 1)[-1]
    return f"[example: {name}, {parts[0]}]" if len(parts) > 1 else f"[example: {name}]"


def _liquid(text: str) -> str:
    out: list[str] = []
    pos = 0
    raw = comment = False
    for m in _LIQUID.finditer(text):
        name, args = m.group(1), m.group(2).strip()
        if raw and name != "endraw":
            continue  # inside {% raw %}: everything is literal text
        if comment and name != "endcomment":
            continue
        if not comment:
            out.append(text[pos : m.start()])
        pos = m.end()
        if name == "raw":
            raw = True
        elif name == "endraw":
            raw = False
        elif name == "comment":
            comment = True
        elif name == "endcomment":
            comment = False
        elif name == "highlight":
            out.append(f"```{args.split()[0] if args else ''}")
        elif name == "endhighlight":
            out.append("```")
        elif name == "include_example":
            out.append(_example_pointer(args))
        elif name in _DROP_TAGS:
            pass
        else:
            raise ValueError(f"unknown Liquid tag {{% {name} %}}")
    if raw or comment:
        raise ValueError("unclosed {% raw %} or {% comment %}")
    if not comment:
        out.append(text[pos:])
    return "".join(out)


# --- 3 & 4. hidden blocks and code masking -----------------------------------------------------------------

_D_NONE = re.compile(r'<div class="d-none"[^>]*>.*?</div>', re.S)
# CommonMark fences: optional indent and blockquote markers, then 3+ backticks or tildes. A backtick fence's
# info string has no backticks (so "```code``` in a sentence" is inline code, not a fence). A closer uses the
# opener's character, is at least as long, and has no info string.
_FENCE_OPEN = re.compile(r"^\s*(?:>\s*)*(`{3,}(?=[^`]*$)|~{3,})(.*)$")
_FENCE_CLOSE = re.compile(r"^\s*(?:>\s*)*(`{3,}|~{3,})\s*$")


def fenced_blocks(lines: list[str]) -> list[tuple[int, int]]:
    """(first, last) line index of every fenced code block. Raises on a fence that is never closed."""
    blocks = []
    i = 0
    while i < len(lines):
        m = _FENCE_OPEN.match(lines[i])
        if m:
            j = i + 1
            while j < len(lines):
                c = _FENCE_CLOSE.match(lines[j])
                if c and c.group(1)[0] == m.group(1)[0] and len(c.group(1)) >= len(m.group(1)):
                    break
                j += 1
            if j == len(lines):
                raise ValueError(f"unclosed code fence at line {i + 1}: {lines[i].strip()[:60]!r}")
            blocks.append((i, j))
            i = j + 1
        else:
            i += 1
    return blocks


_PRE = re.compile(r"<(pre|xmp)\b[^>]*>.*?</\1>", re.S | re.I)
_INLINE_CODE = re.compile(r"`[^`\n]+`")
_INLINE_MASK = "\x00{}\x00"  # inline code: stays inside its line
_BLOCK_MASK = "\x01{}\x01"  # code block: must start on its own line when restored


def _mask_code(text: str) -> tuple[str, list[str]]:
    """Replace code with numbered placeholders. Fenced blocks are found line by line, so an unclosed fence
    raises instead of swallowing the rest of the page."""
    saved: list[str] = []

    def keep(s: str, mask: str = _BLOCK_MASK) -> str:
        saved.append(s)
        return mask.format(len(saved) - 1)

    lines = text.split("\n")
    out: list[str] = []
    i = 0
    for first, last in fenced_blocks(lines):
        out += lines[i:first]
        out.append(keep("\n".join(lines[first : last + 1])))
        i = last + 1
    out += lines[i:]
    text = "\n".join(out)
    text = _PRE.sub(
        lambda m: keep("```\n" + html.unescape(re.sub(r"</?(pre|xmp)\b[^>]*>", "", m.group(0))) + "\n```"),
        text,
    )
    return _INLINE_CODE.sub(lambda m: keep(m.group(0), _INLINE_MASK), text), saved


def _unmask(text: str, saved: list[str]) -> str:
    return re.sub(r"[\x00\x01](\d+)[\x00\x01]", lambda m: _unmask(saved[int(m.group(1))], saved), text)


# --- 5. HTML outside code ----------------------------------------------------------------------------------

# Tags stripped with their text kept. Anything else that looks like a tag is left alone, because outside code
# it is usually prose such as `<driver-node>`; residue counts real HTML element names that survive.
_ALLOWED_TAGS = "|".join(
    ["table", "thead", "tbody", "tfoot", "tr", "td", "th", "div", "p", "br", "b", "i", "em", "strong", "code"]
    + ["a", "img", "ul", "ol", "li", "span", "sup", "sub", "h[1-6]", "hr", "dl", "dt", "dd", "u"]
    + ["small", "big", "kbd", "tt", "font", "center", "strike", "del", "ins", "mark", "abbr", "cite"]
    + ["var", "samp", "details", "summary", "caption", "figure", "figcaption", "col", "colgroup", "section"]
    + ["header", "footer", "nav", "label", "blockquote", "nobr", "wbr"]
)
# A tag name ends at whitespace, '/' or '>': `<col-name>` and `<label-key>` are prose placeholders, not tags.
_NAME_END = r"(?=[\s/>])"
_TAG = re.compile(rf"</?(?:{_ALLOWED_TAGS}){_NAME_END}[^<>]*?/?>", re.I)
_COMMENT = re.compile(r"<!--.*?-->", re.S)
# Every HTML element name: what the residue check looks for after stripping (before entities are unescaped,
# so `&lt;U&gt;` in prose is not mistaken for a tag). Attributes must be name=value, so a prose placeholder
# such as `<time interval>` is not taken for a tag either.
_HTML_ELEMENTS = re.compile(
    r"</?(?:a|abbr|address|area|article|aside|audio|b|base|bdi|bdo|big|blockquote|body|br|button|canvas"
    r"|caption|center|cite|code|col|colgroup|data|datalist|dd|del|details|dfn|dialog|div|dl|dt|em|embed"
    r"|fieldset|figcaption|figure|font|footer|form|h[1-6]|head|header|hr|html|i|iframe|img|input|ins|kbd"
    r"|label|legend|li|link|main|map|mark|menu|meta|meter|nav|nobr|noscript|object|ol|optgroup|option"
    r"|output|p|param|picture|pre|progress|rp|rt|ruby|samp|script|section|select|slot|small|source"
    r"|span|strike|strong|style|sub|summary|sup|svg|table|tbody|td|template|textarea|tfoot|th|thead|time"
    r"|title|tr|track|tt|u|ul|var|video|wbr|xmp)"
    r"(?:\s+[\w:-]+\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s\"'<>]+))*\s*/?>",
    re.I,
)
_INNER_TABLE = re.compile(
    r"<table(?=[\s>])[^>]*>((?:(?!<table[\s>]).)*?)</table\s*>", re.S | re.I
)  # no table inside
_ROW = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
_CELL = re.compile(r"<(t[dh])\b([^>]*)>(.*?)</t[dh]>", re.S | re.I)
_LANG_TAB = re.compile(r'<div\s+data-lang="([^"]+)"[^>]*>', re.I)
_FENCE_LANG = re.compile(r"(?:`{3,}|~{3,})[ \t]*([\w+#.-]+)")
_BLOCK_REF = re.compile(r"\x01(\d+)\x01")
_WHITESPACE_ENTITY = re.compile(r"&(?:nbsp|#160|#x0*a0|#10|#x0*a|#13|#x0*d|#9|#x0*9|NewLine|Tab);|\xa0", re.I)


def _inline_html(fragment: str) -> str:
    """Text of an HTML fragment on one line: inline code kept as `code`, list items and paragraphs joined,
    allowlisted tags removed. Entities are left escaped (the page is unescaped once, after the residue
    count)."""
    fragment = re.sub(r"<code\b[^>]*>(.*?)</code>", r"`\1`", fragment, flags=re.S | re.I)
    fragment = re.sub(r"</li>\s*(?=<li\b)", "; ", fragment, flags=re.I)
    fragment = re.sub(r"<br\s*/?>|</?p\b[^>]*>", " ", fragment, flags=re.I)
    fragment = re.sub(r"<img\b[^>]*>", "", fragment, flags=re.I)
    return _TAG.sub("", fragment)


def _span(attrs: str, name: str) -> int:
    m = re.search(rf"\b{name}\s*=\s*[\"']?(\d+)", attrs, re.I)
    return max(int(m.group(1)), 1) if m else 1


def _fill_spans(cells: list[str], pending: dict[int, tuple[int, str]]) -> None:
    """Repeat the cells of earlier rows whose rowspan still covers the next column(s) of this row."""
    while len(cells) in pending:
        left, text = pending.pop(len(cells))
        if left > 1:
            pending[len(cells)] = (left - 1, text)
        cells.append(text)


class _Tables:
    """Renders the page's HTML tables, innermost first. Code blocks found in cells are either turned into
    inline code (one line) or moved after the whole table (longer); `consumed` records the blocks turned
    into inline code, so the conservation check in clean_page knows they were not lost."""

    def __init__(self, saved: list[str]) -> None:
        self.saved = saved
        self.consumed: set[int] = set()

    def render(self, text: str) -> str:
        while m := _INNER_TABLE.search(text):
            before = _COMMENT.sub("", text[: m.start()])
            opened = len(re.findall(r"<table[\s>]", before, re.I)) - len(
                re.findall(r"</table\s*>", before, re.I)
            )
            nested = opened > 0
            text = text[: m.start()] + self._table(m.group(1), nested) + text[m.end() :]
        return text

    def _cell(self, content: str, lifted: list[str]) -> str:
        def block(m: re.Match[str]) -> str:
            code = self.saved[int(m.group(1))]
            lines = code.split("\n")
            inner = [line for line in lines[1:-1] if line.strip()]
            if len(inner) == 1 and "`" not in inner[0]:  # one line of code: keep it in its cell
                self.consumed.add(int(m.group(1)))
                self.saved.append(f"`{inner[0].strip()}`")
                return _INLINE_MASK.format(len(self.saved) - 1)
            lifted.append(m.group(0))
            return "(code below the table)"

        content = _BLOCK_REF.sub(block, content)
        # <code><xmp>...</xmp></code>: the placeholder is already code, so drop the <code> around it.
        content = re.sub(r"<code\b[^>]*>\s*(\x00\d+\x00|\(code below the table\))\s*</code>", r"\1", content)
        # Entities stay escaped until the page is unescaped, so turn the whitespace ones into spaces here, or
        # an encoded line break would split the row and &nbsp; runs would survive into the chunk text.
        content = _WHITESPACE_ENTITY.sub(" ", _inline_html(content))
        return " ".join(content.split())

    def _table(self, body: str, nested: bool) -> str:
        caption = ""
        if c := re.search(r"<caption\b[^>]*>(.*?)</caption\s*>", body, re.S | re.I):
            caption = self._cell(c.group(1), [])
            body = body[: c.start()] + body[c.end() :]
        rows = _ROW.findall(body)
        if (
            not rows
        ):  # a layout table (no rows, as on the streaming pages): keep its cells as ordinary content
            return (
                "\n\n" + re.sub(r"</?(?:t[dh]|thead|tbody|tfoot)\b[^>]*>", "\n\n", body, flags=re.I) + "\n\n"
            )
        lines: list[str] = []
        lifted: list[str] = []
        pending: dict[int, tuple[int, str]] = {}  # column -> (rows still covered, text) of a rowspan cell
        widest = 0
        for n, row in enumerate(rows):
            cells: list[str] = []
            for _tag, attrs, content in _CELL.findall(row):
                _fill_spans(cells, pending)
                text = self._cell(content, lifted)
                if _span(attrs, "rowspan") > 1:
                    pending[len(cells)] = (_span(attrs, "rowspan") - 1, text)
                cells += [text] + [""] * (_span(attrs, "colspan") - 1)
            _fill_spans(cells, pending)
            while cells and not cells[-1]:
                cells.pop()
            if not cells:
                continue
            widest = max(widest, len(cells))
            if nested:
                lines.append(", ".join(c for c in cells if c))
                continue
            lines.append(" | ".join(cells))
            if n == 0 and re.search(r"<th\b", row, re.I) and not re.search(r"<td\b", row, re.I):
                lines.append(" | ".join("---" for _ in cells))
        if nested:  # a table inside a cell: one line, rows separated by semicolons
            return f"{caption} " * bool(caption) + "; ".join(lines) + "".join(f" {b}" for b in lifted)
        if widest == 1:  # one column: without pipes, 'A\n---' would not be a table
            lines = [f"| {line} |" for line in lines]
        head = f"\n\n{caption}" if caption else ""
        return head + "\n\n" + "\n".join(lines) + "\n\n" + "".join(f"{b}\n\n" for b in lifted)


def _tab_label(m: re.Match[str], saved: list[str]) -> str:
    """The `<lang>:` label line of a language tab, left out when the tab opens with a code block fenced in the
    same language: it would only repeat the fence, and between two pieces of a split block it was a tiny chunk
    of its own (D-012)."""
    lang = m.group(1)
    ref = re.match(r"\s*\x01(\d+)\x01", m.string[m.end() :])
    if (
        ref
        and (fence := _FENCE_LANG.match(saved[int(ref.group(1))]))
        and fence.group(1).lower() == lang.lower()
    ):
        return "\n\n"
    return f"\n\n{lang}:\n\n"


def _html(text: str, tables: _Tables) -> str:
    """HTML outside code to text. Entities stay escaped; clean_page unescapes after counting residue."""
    text = re.sub(r"<(style|script)\b[^>]*>.*?</\1\s*>", "", text, flags=re.S | re.I)
    text = tables.render(text)
    text = _LANG_TAB.sub(lambda m: _tab_label(m, tables.saved), text)
    text = re.sub(r"<li\b[^>]*>", "\n- ", text, flags=re.I)
    # In a Markdown pipe-table row a <br> is a line break inside a cell, not a new row.
    text = re.sub(r"(?m)^[ \t]*\|.*$", lambda m: re.sub(r"<br\s*/?>", " ", m.group(0), flags=re.I), text)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    blocks = "p|div|ul|ol|dl|hr|details|summary|blockquote|section|figure|figcaption|center|header|footer|nav"
    text = re.sub(rf"</?(?:{blocks}){_NAME_END}[^>]*>", "\n\n", text, flags=re.I)
    text = re.sub(r"<code\b[^>]*>(.*?)</code>", r"`\1`", text, flags=re.S | re.I)
    text = re.sub(r"<img\b[^>]*>", "", text, flags=re.I)
    return _TAG.sub("", text)


# --- 6. Markdown and kramdown leftovers --------------------------------------------------------------------


# A link destination: <...>, or no spaces with one level of parentheses (wiki/Lasso_(statistics),
# Dataset.html#f():D[T]) possibly wrapped onto the next line; then an optional "title", 'title' or (title).
_LINK_URL = (
    r"\([ \t]*(?:<[^<>\n]*>|(?:[^()\s]|\n|\([^()\s]*\))*)"
    r"(?:[ \t]+(?:\"[^\"\n]*\"|'[^'\n]*'|\([^()\n]*\)))?[ \t]*\)"
)
_NEVER_CLOSED = r"\((?:https?://|/)[^\s()]*(?=[ \t]*(?:\n|$))"  # a URL that runs to the end of the line
_LINK_LEFT = re.compile(rf"\]{_LINK_URL}|\]{_NEVER_CLOSED}")


def _footnote(m: re.Match[str]) -> str:
    return "Note: " + " ".join(m.group(1).split())


def _markdown(text: str) -> str:
    text = re.sub(r"^.*\n(?=\{:toc\}\s*$)", "", text, flags=re.M)  # the bullet line before {:toc}
    text = re.sub(r"^\s*\{:[^}\n]*\}\s*$", "", text, flags=re.M)
    # kramdown footnotes: the definition (with its indented continuation lines) becomes a plain note, the
    # markers in the text go.
    text = re.sub(r"(?m)^ {0,3}\[\^[^\]\n]+\]:[ \t]*(.*(?:\n(?:[ \t]{2,}|\t)\S.*)*)", _footnote, text)
    text = re.sub(r"[ \t]?\[\^[^\]\n]+\]", "", text)
    text = re.sub(r"^ {0,3}\[[^\]\n]+\]:[ \t]+\S+.*$", "", text, flags=re.M)  # reference-link definitions
    text = re.sub(r"(?m)^ {0,3}([*_])(?:[ \t]*\1){2,}[ \t]*$", "", text)  # *** and ___ horizontal rules
    url = _LINK_URL
    text = re.sub(rf"!\[[^\]\n]*\]{url}", "", text)  # images
    # Link text may hold one level of brackets ("[[SPARK-1]](url)") and wrap onto the next line, but not
    # cross a blank line.
    link_text = r"((?:\[[^\]\n]*\]|[^\[\]\n]|\n(?!\s*\n))+)"
    text = re.sub(rf"\[{link_text}\]{url}", r"\1", text)  # [text](url) -> text
    text = re.sub(rf"\[{link_text}\]{_NEVER_CLOSED}", r"\1", text)  # never closed
    text = re.sub(rf"\[{link_text}\]\[[^\]\n]*\]", r"\1", text)  # [text][ref] -> text
    return text


def clean_inline(text: str) -> str:
    """Clean a single line such as a heading or title: markup removed, whitespace collapsed."""
    masked, saved = _mask_code(text)
    text = _unmask(_markdown(html.unescape(_COMMENT.sub("", _inline_html(masked)))), saved)
    return " ".join(re.sub(r"\s*\{#[^}]*\}\s*$", "", text).split())


# --- 8. table borders and macro preambles ----------------------------------------------------------------

_BORDER_LINE = re.compile(r"[|+:= \t-]*")
_LONG_RULE = re.compile(r"([-=])\1{3,}")
_MATHJAX_PREAMBLE = re.compile(r"`\\\[\s*(?:\\newcommand\{[^}\n]*\}\{[^\n]*\}\s*)+\\\]`")


def _shorten_borders(line: str) -> str:
    return _LONG_RULE.sub(r"\1\1\1", line) if _BORDER_LINE.fullmatch(line) else line


# --- the whole page ----------------------------------------------------------------------------------------


# A GFM table's delimiter row: cells of dashes (optionally colon-aligned) separated by pipes.
DELIMITER_ROW = re.compile(r"\s*\|?\s*:?-+:?\s*(?:\|\s*:?-+:?\s*)+\|?\s*|\s*\|\s*:?-+:?\s*\|\s*")


@dataclass(frozen=True)
class CleanDoc:
    title: str
    text: str
    residue: dict[str, int]  # markup left after cleaning, counted outside code; the silver run fails if > 0


def _mask_ids(text: str) -> set[int]:
    return {int(i) for i in re.findall(r"[\x00\x01](\d+)[\x00\x01]", text)}


def _present(text: str, saved: list[str]) -> set[int]:
    """Placeholders in the text, and those nested in their saved code (a <pre> inside a code span)."""
    found: set[int] = set()
    todo = _mask_ids(text)
    while todo:
        i = todo.pop()
        if i not in found:
            found.add(i)
            todo |= _mask_ids(saved[i])
    return found


# What cleaning drops on purpose, code included: comments, <style>/<script>, image alt text, reference
# definitions (their titles).
_DROPPED = re.compile(
    r"<!--.*?-->|<(style|script)\b[^>]*>.*?</\1\s*>|!\[[^\]\n]*\]|^ {0,3}\[(?!\^)[^\]\n]+\]:[ \t]+\S+.*$",
    re.S | re.I | re.M,
)


def _broken_tables(lines: list[str]) -> int:
    """Delimiter rows that do not sit right under a one-line header that opens its paragraph: the chunker
    could not repeat that table's header (a header broken over lines, or a table glued to a paragraph)."""
    return sum(
        1
        for i, line in enumerate(lines)
        if "|" in line
        and DELIMITER_ROW.fullmatch(line)
        and not (i >= 1 and "|" in lines[i - 1] and (i == 1 or _opens_paragraph(lines[i - 2])))
    )


def _opens_paragraph(line: str) -> bool:
    """A table header right after this line starts a unit of its own for the chunker."""
    return not line.strip() or bool(re.match(r" {0,3}#{1,6}(?:\s|$)|\x01\d+\x01$", line))


def clean_page(raw: str, fallback_title: str) -> CleanDoc:
    meta, body = split_front_matter(raw)
    title = clean_inline(meta.get("displayTitle") or meta.get("title") or fallback_title)
    body = _liquid(body)
    body = _D_NONE.sub("", body)
    body = _MATHJAX_PREAMBLE.sub("", body)
    masked, saved = _mask_code(body)
    every_mask = set(range(len(saved)))
    dropped = {i for m in _DROPPED.finditer(masked) for i in _mask_ids(m.group(0))}  # dropped on purpose
    tables = _Tables(saved)
    masked = _COMMENT.sub("", _html(masked, tables))  # before unescaping: '&lt;!--' in prose stays prose
    html_tags = len(_HTML_ELEMENTS.findall(masked))
    masked = _markdown(html.unescape(masked))
    lost = every_mask - _present(masked, saved) - tables.consumed - dropped
    if lost:  # code is never dropped by cleaning, only moved; losing it means a rule swallowed it
        examples = [saved[i][:60] for i in sorted(lost)[:3]]
        raise ValueError(f"cleaning lost {len(lost)} code spans or blocks, e.g. {examples}")
    final_lines = [line.rstrip() for line in masked.split("\n")]
    found = {
        "liquid_tags": len(re.findall(r"\{%", masked)),
        "html_tags": html_tags,
        "markdown_links": len(_LINK_LEFT.findall(masked)),  # link syntax only; "[a](b c)" is literal text
        "broken_table_headers": _broken_tables(final_lines),
    }
    text = _unmask(masked, saved)
    text = "\n".join(_shorten_borders(line.rstrip()) for line in text.split("\n"))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return CleanDoc(title=title, text=text, residue=found)
