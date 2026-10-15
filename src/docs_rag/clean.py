"""Turn one Spark docs page (Jekyll Markdown with Liquid and HTML) into clean Markdown text.

The order matters and is fixed (D-008):
1. split off the YAML front matter; the title comes from `displayTitle` (else `title`);
2. Liquid, from an explicit table: `highlight` blocks become fenced code, `include_example` becomes a one-line
   pointer, generated includes and control flow are dropped, `raw` markers are dropped with their content
   kept, `comment` blocks are dropped. An unknown Liquid tag raises: guessing would put junk in chunk text;
3. hidden `<div class="d-none">` blocks are dropped with their content (they only hold markup for viewers);
4. code (fenced blocks, `<pre>`/`<xmp>` blocks, inline code spans) is masked, so nothing below touches it.
   Code is full of angle brackets that are not HTML (`Dataset<Row>`, XML config, `<driver-node>`);
5. outside code: HTML tables become one ` | `-joined line per row, language tabs get a `<lang>:` label line,
   only an allowlist of real HTML tag names is stripped (text kept), then entities are unescaped;
6. Markdown links keep their text, images and reference-link definitions go, kramdown `{:...}` lines and the
   table-of-contents bullet before `{:toc}` go, HTML comments go;
7. code is restored and whitespace normalised.

`{{ site.X }}` variables are left literal on purpose: substituting the tag's version would make an unchanged
page produce different text in every tag and change its chunk IDs (M3, M5).
"""

import html
import re
from dataclasses import dataclass

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
            out.append(f"[example code: {args.split()[-1]}]" if args else "")
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

_ALLOWED_TAGS = "|".join(
    ["table", "thead", "tbody", "tfoot", "tr", "td", "th", "div", "p", "br", "b", "i", "em", "strong", "code"]
    + ["a", "img", "ul", "ol", "li", "span", "sup", "sub", "h[1-6]", "hr", "dl", "dt", "dd"]
)
_TAG = re.compile(rf"</?(?:{_ALLOWED_TAGS})\b[^<>]*?/?>", re.I)
_TABLE = re.compile(r"<table\b[^>]*>(.*?)</table>", re.S | re.I)
_ROW = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
_CELL = re.compile(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", re.S | re.I)
_LANG_TAB = re.compile(r'<div\s+data-lang="([^"]+)"[^>]*>', re.I)


def _inline_html(fragment: str) -> str:
    """Text of an HTML fragment: inline code kept as `code`, allowlisted tags removed, entities unescaped."""
    fragment = re.sub(r"<code\b[^>]*>(.*?)</code>", r"`\1`", fragment, flags=re.S | re.I)
    fragment = re.sub(r"<br\s*/?>", " ", fragment, flags=re.I)
    fragment = re.sub(r"<img\b[^>]*>", "", fragment, flags=re.I)
    return html.unescape(_TAG.sub("", fragment))


def _table(m: re.Match[str]) -> str:
    """One line per row; a code block inside a cell is lifted out onto its own lines after the row."""
    rows = []
    for row in _ROW.findall(m.group(1)):
        cells, blocks = [], []
        for cell in _CELL.findall(row):
            blocks += re.findall(r"\x01\d+\x01", cell)
            cells.append(" ".join(_inline_html(re.sub(r"\x01\d+\x01", " ", cell)).split()))
        if any(cells):
            rows.append(" | ".join(cells))
        rows += [f"\n{b}\n" for b in blocks]
    return "\n\n" + "\n".join(rows) + "\n\n"


def _html(text: str) -> str:
    text = _TABLE.sub(_table, text)
    text = _LANG_TAB.sub(lambda m: f"\n\n{m.group(1)}:\n\n", text)
    text = re.sub(r"<li\b[^>]*>", "\n- ", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|ul|ol|dl|hr)\b[^>]*>", "\n\n", text, flags=re.I)
    text = re.sub(r"<code\b[^>]*>(.*?)</code>", r"`\1`", text, flags=re.S | re.I)
    text = re.sub(r"<img\b[^>]*>", "", text, flags=re.I)
    return html.unescape(_TAG.sub("", text))


# --- 6. Markdown and kramdown leftovers --------------------------------------------------------------------


def _markdown(text: str) -> str:
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    text = re.sub(r"^.*\n(?=\{:toc\}\s*$)", "", text, flags=re.M)  # the bullet line before {:toc}
    text = re.sub(r"^\s*\{:[^}\n]*\}\s*$", "", text, flags=re.M)
    text = re.sub(r"^ {0,3}\[[^\]\n]+\]:[ \t]+\S+.*$", "", text, flags=re.M)  # reference-link definitions
    text = re.sub(r"!\[[^\]\n]*\]\([^)\n]*\)", "", text)  # images
    # Link text may hold one level of brackets ("[[SPARK-1]](url)") and wrap onto the next line, but not
    # cross a blank line.
    link_text = r"((?:\[[^\]\n]*\]|[^\[\]\n]|\n(?!\s*\n))+)"
    text = re.sub(rf"\[{link_text}\]\([^)\n]*\)", r"\1", text)  # [text](url) -> text
    text = re.sub(rf"\[{link_text}\]\[[^\]\n]*\]", r"\1", text)  # [text][ref] -> text
    return text


def clean_inline(text: str) -> str:
    """Clean a single line such as a heading or title: markup removed, whitespace collapsed."""
    masked, saved = _mask_code(text)
    text = _unmask(_markdown(_inline_html(masked)), saved)
    return " ".join(re.sub(r"\s*\{#[^}]*\}\s*$", "", text).split())


# --- the whole page ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CleanDoc:
    title: str
    text: str


def clean_page(raw: str, fallback_title: str) -> CleanDoc:
    meta, body = split_front_matter(raw)
    title = clean_inline(meta.get("displayTitle") or meta.get("title") or fallback_title)
    body = _liquid(body)
    body = _D_NONE.sub("", body)
    masked, saved = _mask_code(body)
    masked = _markdown(_html(masked))
    text = _unmask(masked, saved)
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return CleanDoc(title=title, text=text)


def residue(text: str) -> dict[str, int]:
    """Markup that should never survive cleaning, counted outside code. The silver job fails if any is > 0."""
    masked, _ = _mask_code(text)
    return {
        "liquid_tags": len(re.findall(r"\{%", masked)),
        "html_tags": len(_TAG.findall(masked)),
        "markdown_links": len(re.findall(r"\]\([^)\n]*\)", masked)),
    }
