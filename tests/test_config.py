import dataclasses

import pytest

from docs_rag.config import Config


def test_defaults_name_the_dev_tables() -> None:
    assert Config().bronze_table == "workspace.docs_rag_dev.bronze_docs"
    assert Config(schema="docs_rag_prod").table("silver_chunks") == "workspace.docs_rag_prod.silver_chunks"


@pytest.mark.parametrize(
    "field,value",
    [
        ("catalog", "work space"),
        ("schema", "docs_rag_dev; DROP TABLE x"),
        ("schema", "1abc"),
        ("source_repo", "apache"),
        ("docs_dir", "docs/../etc"),
        ("doc_suffix", "md"),
    ],
)
def test_rejects_values_that_could_break_sql_or_paths(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="invalid"):
        Config(**{field: value})


def test_volume_path_is_in_the_configured_schema() -> None:
    assert Config(schema="docs_rag_prod").volume("artifacts") == "/Volumes/workspace/docs_rag_prod/artifacts"


def test_table_name_is_validated() -> None:
    with pytest.raises(ValueError, match="invalid table name"):
        Config().table("bronze docs")


def test_config_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        Config().schema = "other"  # type: ignore[misc]
