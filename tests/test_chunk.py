import json
import os
import random
import re
import subprocess
import sys
from pathlib import Path

import pytest

from docs_rag.chunk import ChunkConfig, chunk_document, chunk_id, chunk_section
from docs_rag.tokens import count_tokens

FIXTURE = Path(__file__).parent / "fixtures" / "spark_page.md"
SMALL = ChunkConfig(max_tokens=64, overlap_tokens=8)
WORDS = [
    "spark",
    "executor",
    "driver",
    "shuffle",
    "partition",
    "the",
    "of",
    "a",
    "to",
    "is",
    "DataFrame",
    "spark.sql.shuffle.partitions",
    "memoryOverhead",
    "`SparkSession`",
    "(default)",
    "200,",
    "e.g.",
    "org.apache.spark.sql.Dataset[Row]",
    "-1",
    ">=",
    "JSON",
]


def prose(rng: random.Random, sentences: int) -> str:
    return " ".join(
        " ".join(rng.choice(WORDS) for _ in range(rng.randint(3, 40))).capitalize() + "."
        for _ in range(sentences)
    )


def random_doc(rng: random.Random) -> str:
    parts = []
    for _ in range(rng.randint(1, 6)):
        kind = rng.random()
        if kind < 0.2:
            parts.append("#" * rng.randint(1, 4) + " " + prose(rng, 1)[:40])
        elif kind < 0.4:
            lines = [prose(rng, 1)[:60] if rng.random() < 0.9 else "" for _ in range(rng.randint(1, 60))]
            parts.append("```python\n" + "\n".join(lines) + "\n```")
        elif kind < 0.45:
            parts.append("".join(rng.choice("abcdefghij0123456789") for _ in range(rng.randint(50, 99))))
        else:
            parts.append(prose(rng, rng.randint(1, 30)))
    return "\n\n".join(parts) + "\n"


def bodies(markdown: str, config: ChunkConfig = SMALL) -> list[str]:
    return [c.body for c in chunk_document("docs/x.md", markdown, config).chunks]


# --- config ---


def test_config_bounds() -> None:
    with pytest.raises(ValueError, match="max_tokens"):
        ChunkConfig(max_tokens=511)
    with pytest.raises(ValueError, match="overlap_tokens"):
        ChunkConfig(max_tokens=64, overlap_tokens=32)


def test_config_id_changes_with_every_parameter() -> None:
    ids = {
        ChunkConfig().config_id,
        ChunkConfig(max_tokens=200).config_id,
        ChunkConfig(overlap_tokens=0).config_id,
    }
    assert len(ids) == 3


# --- shape of the output ---


def test_small_section_is_one_chunk_with_its_breadcrumb() -> None:
    doc = chunk_document("docs/x.md", "---\ntitle: Tuning\n---\n# Memory\n\nUse less of it.\n", SMALL)
    assert [(c.section_path, c.body, c.embed_text) for c in doc.chunks] == [
        (("Tuning", "Memory"), "Use less of it.", "Tuning > Memory\n\nUse less of it.")
    ]


def test_empty_sections_and_generated_only_pages_give_no_chunks() -> None:
    doc = chunk_document("docs/f.md", "# Aggregate\n{% include_api_gen gen.html %}\n## Examples\n", SMALL)
    assert doc.chunks == ()
    assert doc.sections == 3 and doc.empty_sections == 3


def test_fixture_page() -> None:
    doc = chunk_document("docs/quick-start.md", FIXTURE.read_text(), ChunkConfig())
    assert doc.residue == {"liquid_tags": 0, "html_tags": 0, "markdown_links": 0}
    assert [c.section_path[-1] for c in doc.chunks] == ["Quick Start", "Basics", "Spark Properties", "TF-IDF"]
    assert doc.chunks[2].embed_text.endswith("in the UI and in log data. | 0.9.0")


# --- properties over random documents ---


@pytest.mark.parametrize("seed", range(40))
def test_every_chunk_fits_and_its_count_is_a_recount(seed: int) -> None:
    rng = random.Random(seed)
    config = ChunkConfig(max_tokens=rng.choice([32, 64, 128, 254]), overlap_tokens=rng.choice([0, 4, 8]))
    doc = chunk_document("docs/x.md", random_doc(rng), config)
    for c in doc.chunks:
        assert c.n_tokens == count_tokens(c.embed_text) <= config.max_tokens
        assert c.body.strip() == c.body and c.body
    assert len({c.chunk_id for c in doc.chunks}) == len(doc.chunks)


@pytest.mark.parametrize("seed", range(20))
def test_without_overlap_prose_is_cut_at_word_boundaries_and_nothing_is_lost(seed: int) -> None:
    rng = random.Random(seed)
    text = prose(rng, rng.randint(5, 60))
    pieces = bodies(f"# H\n\n{text}\n", ChunkConfig(max_tokens=32, overlap_tokens=0))
    assert re.sub(r"\s", "", "".join(pieces)) == re.sub(r"\s", "", text)
    end = 0
    for piece in pieces:  # each piece is a verbatim slice of the text; no cut splits letters or digits
        start = text.index(piece, end)
        assert not (0 < start == end and text[start - 1].isalnum() and text[start].isalnum()), piece[:20]
        end = start + len(piece)


def test_continuations_of_a_split_paragraph_repeat_the_previous_tail() -> None:
    """Overlap is best effort: it needs a whitespace start within the last 8 tokens and room in the budget."""
    continuations = overlapping = 0
    for seed in range(10):
        text = prose(random.Random(seed), 40)
        pieces = bodies(f"# H\n\n{text}\n", ChunkConfig(max_tokens=64, overlap_tokens=8))
        starts = [text.index(pieces[0])]
        for piece in pieces[1:]:
            starts.append(text.index(piece, starts[-1] + 1))
        for left, start, next_start in zip(pieces, starts, starts[1:], strict=False):
            end = start + len(left)
            continuations += 1
            if next_start < end:
                overlapping += 1
                assert start < next_start and text[next_start - 1] == " "
                assert 0 < count_tokens(text[next_start:end]) <= 8
    assert continuations > 100 and overlapping / continuations > 0.8


def test_a_code_block_that_fits_stays_whole() -> None:
    code = "```python\n" + "\n".join(f"x{i} = {i}" for i in range(8)) + "\n\nprint(x0)\n```"
    assert count_tokens(code) < 60
    chunks = bodies(f"# H\n\n{prose(random.Random(1), 3)}\n\n{code}\n", ChunkConfig(max_tokens=80))
    assert any(code in b for b in chunks)
    assert sum(b.count("```") for b in chunks) == 2


def test_a_long_code_block_is_split_into_fenced_pieces() -> None:
    inner = [f"spark.conf.set('spark.sql.shuffle.partitions', {i})" for i in range(40)]
    pieces = bodies(
        "# H\n\n```python\n" + "\n".join(inner) + "\n```\n", ChunkConfig(max_tokens=64, overlap_tokens=0)
    )
    assert len(pieces) > 1
    assert all(p.startswith("```python\n") and p.endswith("\n```") for p in pieces)
    assert [line for p in pieces for line in p.split("\n")[1:-1]] == inner


def test_a_word_longer_than_the_budget_is_cut_and_still_fits() -> None:
    word = "".join(random.Random(3).choice("bcdfghjklmnpqrstvwxz") for _ in range(99))
    doc = chunk_document("docs/x.md", f"# H\n\n{word} {word}\n", ChunkConfig(max_tokens=24, overlap_tokens=4))
    assert len(doc.chunks) > 2 and all(c.n_tokens <= 24 for c in doc.chunks)


def test_a_breadcrumb_that_leaves_no_room_raises() -> None:
    with pytest.raises(ValueError, match="breadcrumb"):
        chunk_section("docs/x.md", ("word " * 30,), "body", ChunkConfig(max_tokens=32, overlap_tokens=0))


# --- chunk identity ---


def ids(markdown: str, path: str = "docs/x.md") -> dict[tuple[str, ...], list[str]]:
    out: dict[tuple[str, ...], list[str]] = {}
    for c in chunk_document(path, markdown, SMALL).chunks:
        out.setdefault(c.section_path, []).append(c.chunk_id)
    return out


PAGE = "# A\n\nalpha text.\n\n# B\n\nbeta text.\n\n## B1\n\nbeta one.\n"


def test_an_edit_changes_only_the_edited_sections_ids() -> None:
    before, after = ids(PAGE), ids(PAGE.replace("alpha text.", "alpha text, edited."))
    assert before[("x", "A")] != after[("x", "A")]
    assert {k: v for k, v in before.items() if k != ("x", "A")} == {
        k: v for k, v in after.items() if k != ("x", "A")
    }


def test_moving_a_section_keeps_its_ids() -> None:
    moved = "# B\n\nbeta text.\n\n## B1\n\nbeta one.\n\n# A\n\nalpha text.\n"
    assert ids(PAGE) == ids(moved)


def test_renaming_a_heading_changes_the_ids_beneath_it() -> None:
    renamed = ids(PAGE.replace("# B\n", "# Bee\n"))
    assert renamed[("x", "A")] == ids(PAGE)[("x", "A")]
    assert ("x", "B", "B1") not in renamed and ("x", "Bee", "B1") in renamed


def test_ids_depend_on_the_doc_path() -> None:
    assert ids(PAGE, "docs/x.md")[("x", "A")] != ids(PAGE, "docs/y/x.md")[("x", "A")]


def test_identical_chunks_in_one_section_get_distinct_ids() -> None:
    block = "```\n" + " ".join(["same words here."] * 12) + "\n```"
    chunks = chunk_document("docs/x.md", "# A\n\n" + f"{block}\n\n" * 3, SMALL).chunks
    texts = [c.embed_text for c in chunks]
    assert len(set(texts)) < len(texts)  # the test needs repeated chunk texts
    assert len({c.chunk_id for c in chunks}) == len(chunks)
    assert chunks[0].chunk_id == chunk_id("docs/x.md", ("x", "A"), texts[0], 0)


def test_output_does_not_depend_on_the_hash_seed() -> None:
    script = (
        "import json, sys; from docs_rag.chunk import ChunkConfig, chunk_document; "
        "d = chunk_document('docs/q.md', sys.stdin.read(), ChunkConfig(max_tokens=64, overlap_tokens=8)); "
        "print(json.dumps([(c.chunk_id, c.n_tokens) for c in d.chunks]))"
    )
    page = FIXTURE.read_text() + "\n\n## Long\n\n" + prose(random.Random(5), 30)
    outputs = {
        subprocess.run(
            [sys.executable, "-c", script],
            input=page,
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
        ).stdout
        for seed in ("0", "1", "12345")
    }
    assert len(outputs) == 1 and len(json.loads(outputs.pop())) > 5
