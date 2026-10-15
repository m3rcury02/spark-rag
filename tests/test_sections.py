from docs_rag.sections import Section, split_sections


def test_heading_paths_follow_the_heading_levels() -> None:
    text = "intro\n# A\na\n## B\nb\n### C\nc\n## D\nd\n# E\ne"
    assert split_sections("T", text) == [
        Section(("T",), "intro"),
        Section(("T", "A"), "a"),
        Section(("T", "A", "B"), "b"),
        Section(("T", "A", "B", "C"), "c"),
        Section(("T", "A", "D"), "d"),
        Section(("T", "E"), "e"),
    ]


def test_a_heading_with_no_body_is_an_empty_section_that_names_its_children() -> None:
    sections = split_sections("T", "# Parent\n## Child\ntext")
    assert sections == [
        Section(("T",), ""),
        Section(("T", "Parent"), ""),
        Section(("T", "Parent", "Child"), "text"),
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
    assert split_sections("T", "# A\n### C\nc\n## B\nb")[-2:] == [
        Section(("T", "A", "C"), "c"),
        Section(("T", "A", "B"), "b"),
    ]
