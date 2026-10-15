from docs_rag.sections import split_sections


def paths_and_bodies(title: str, text: str) -> list[tuple[tuple[str, ...], str]]:
    sections = split_sections(title, text)
    for s in sections:  # every body sits at its offset in the page text
        assert text[s.start : s.start + len(s.body)] == s.body
    return [(s.path, s.body) for s in sections]


def test_heading_paths_follow_the_heading_levels() -> None:
    text = "intro\n# A\na\n## B\nb\n### C\nc\n## D\nd\n# E\ne"
    assert paths_and_bodies("T", text) == [
        (("T",), "intro"),
        (("T", "A"), "a"),
        (("T", "A", "B"), "b"),
        (("T", "A", "B", "C"), "c"),
        (("T", "A", "D"), "d"),
        (("T", "E"), "e"),
    ]


def test_a_heading_with_no_body_is_an_empty_section_that_names_its_children() -> None:
    assert paths_and_bodies("T", "# Parent\n## Child\n\n  text  \n") == [
        (("T",), ""),
        (("T", "Parent"), ""),
        (("T", "Parent", "Child"), "text"),
    ]


def test_hash_lines_inside_code_are_not_headings() -> None:
    text = "# Real\n```bash\n# a shell comment\n```\n    # indented four spaces: code, not a heading"
    sections = split_sections("T", text)
    assert [s.path for s in sections] == [("T",), ("T", "Real")]
    assert "# a shell comment" in sections[1].body


def test_heading_text_is_cleaned() -> None:
    sections = split_sections(
        "T", "## Launching with [`spark-submit`](x.html) {#launch}\nbody\n## Closed ##\nb"
    )
    assert [s.path for s in sections[1:]] == [("T", "Launching with `spark-submit`"), ("T", "Closed")]


def test_skipped_levels_still_nest() -> None:
    assert paths_and_bodies("T", "# A\n### C\nc\n## B\nb")[-2:] == [
        (("T", "A", "C"), "c"),
        (("T", "A", "B"), "b"),
    ]
