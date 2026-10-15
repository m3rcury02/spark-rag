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


def split_sections(title: str, text: str) -> list[Section]:
    lines = text.split("\n")
    in_code = {i for first, last in fenced_blocks(lines) for i in range(first, last + 1)}
    sections: list[Section] = []
    stack: list[tuple[int, str]] = []
    body: list[str] = []

    def flush() -> None:
        sections.append(Section(path=(title, *(h for _, h in stack)), body="\n".join(body).strip()))

    for i, line in enumerate(lines):
        m = None if i in in_code else _ATX.match(line)
        if m is None:
            body.append(line)
            continue
        flush()
        body = []
        level = len(m.group(1))
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, clean_inline(m.group(2) or "")))
    flush()
    return sections
