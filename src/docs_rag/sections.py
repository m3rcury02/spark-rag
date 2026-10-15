"""Split a cleaned page into sections at its ATX headings (`#` .. `######`), ignoring `#` lines inside code.

Each section carries its heading path: (doc title, h1, h2, ...), the stack of headings above it. A heading
with no body of its own still names its children: it lives on in their paths.
"""

import re
from dataclasses import dataclass

from docs_rag.clean import clean_inline, fenced_blocks

_ATX = re.compile(r"^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$")


@dataclass(frozen=True)
class Section:
    path: tuple[str, ...]  # (doc title, heading, subheading, ...)
    body: str  # may be empty: a heading followed directly by a subheading
    start: int  # offset of body in the page text: text[start : start + len(body)] == body


def split_sections(title: str, text: str) -> list[Section]:
    lines = text.split("\n")
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line) + 1)
    in_code = {i for first, last in fenced_blocks(lines) for i in range(first, last + 1)}
    sections: list[Section] = []
    stack: list[tuple[int, str]] = []
    first = 0  # first line of the current body

    def flush(end: int) -> None:
        raw = text[offsets[first] : max(offsets[end] - 1, offsets[first])]
        body = raw.strip()
        start = offsets[first] + (len(raw) - len(raw.lstrip())) if body else offsets[first]
        sections.append(Section(path=(title, *(h for _, h in stack)), body=body, start=start))

    for i, line in enumerate(lines):
        m = None if i in in_code else _ATX.match(line)
        if m is None:
            continue
        flush(i)
        first = i + 1
        level = len(m.group(1))
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, clean_inline(m.group(2) or "")))
    flush(len(lines))
    return sections
