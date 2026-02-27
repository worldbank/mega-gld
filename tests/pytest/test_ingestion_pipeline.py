import re
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pandas as pd
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from helpers.ingestion_pipeline import *



def _normalize_sql(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

@pytest.mark.parametrize(
    "harm_type,household_level,table_version,expected_fragments,unexpected_fragments",
    [
        # --- Case 1: all NULLs (but ingested is always TRUE) ---
        (
            None, None, None,
            [
                "ingested = TRUE",
                "harmonization = NULL",
                "household_level = NULL",
                "table_version = NULL",
            ],
            [
                "'None'",
                "harmonization = 'None'",
                "household_level = TRUE",
                "household_level = FALSE",
                "table_version = 'None'",
            ],
        ),

        # --- Case 2: typical positive values ---
        (
            "GLD", True, 12,
            [
                "ingested = TRUE",
                "harmonization = 'GLD'",
                "household_level = TRUE",
                "table_version = 12",
            ],
            [
                "harmonization = NULL",
                "household_level = NULL",
                "household_level = FALSE",
                "table_version = NULL",
                "'None'",
            ],
        ),

        # --- Case 3: false + zero ---
        (
            "GLD-Light", False, 0,
            [
                "ingested = TRUE",
                "harmonization = 'GLD-Light'",
                "household_level = FALSE",
                "table_version = 0",
            ],
            [
                "harmonization = NULL",
                "household_level = NULL",
                "household_level = TRUE",
                "table_version = NULL",
                "'None'",
            ],
        ),
    ],
)


def test_update_metadata_builds_expected_sql(
    capsys,
    harm_type,
    household_level,
    table_version,
    expected_fragments,
    unexpected_fragments,
):
    spark = MagicMock()
    metadata_table = "schema._ingestion_metadata"
    dta_path = "/mnt/data/foo.dta"
    tbl_name = "schema.my_table"

    update_metadata(
        spark=spark,
        metadata_table=metadata_table,
        dta_path=dta_path,
        tbl_name=tbl_name,
        harm_type=harm_type,
        household_level=household_level,
        table_version=table_version,
    )

    assert spark.sql.call_count == 1
    sql = _normalize_sql(spark.sql.call_args[0][0])

    assert f"UPDATE {metadata_table}" in sql
    assert "SET ingested = TRUE" in sql
    assert f"table_name = '{tbl_name}'" in sql
    assert f"WHERE dta_path = '{dta_path}'" in sql

    for frag in expected_fragments:
        assert frag in sql
    for frag in unexpected_fragments:
        assert frag not in sql

    out = capsys.readouterr().out
    assert "Updated metadata for:" in out
    assert dta_path in out



def test_get_stata_var_labels_filters_empty_and_none(monkeypatch):
    # Create a fake pyreadstat meta object
    meta = SimpleNamespace(
        column_names=["a", "b", "c", "d"],
        column_labels=["Label A", None, "   ", ""],
    )

    def fake_read_dta(path, metadataonly=True):
        assert metadataonly is True
        return None, meta

    monkeypatch.setattr(pyreadstat, "read_dta", fake_read_dta)

    labels = get_stata_var_labels("/mnt/data/file.dta")
    assert labels == {"a": "Label A"}



def test_apply_column_comments_skips_none_and_sanitizes_and_calls_sql():
    spark = MagicMock()
    full_table_name = "schema.tbl"
    var_labels = {
        "col1": "Hello\nWorld",
        "col2": "bad\x00null\rchars",
        "col3": "O'Reilly",  
        "col4": None,
    }

    apply_column_comments(spark, full_table_name, var_labels)

    # Should call spark.sql 3 times (col4 skipped)
    assert spark.sql.call_count == 3
    calls = [c.args[0] for c in spark.sql.call_args_list]

    # Verify sanitized labels appear
    joined = "\n".join(calls)
    assert "ALTER TABLE schema.tbl ALTER COLUMN `col1` COMMENT 'Hello World'" in joined
    assert "ALTER TABLE schema.tbl ALTER COLUMN `col2` COMMENT 'bad null chars'" in joined
    assert "ALTER TABLE schema.tbl ALTER COLUMN `col3` COMMENT 'OReilly'" in joined


def test_apply_column_comments_catches_exceptions_and_continues(capsys):
    spark = MagicMock()
    full_table_name = "schema.tbl"
    var_labels = {"col1": "ok", "col2": "boom", "col3": "still ok"}

    def sql_side_effect(stmt):
        if "`col2`" in stmt:
            raise RuntimeError("fail")
        return None

    spark.sql.side_effect = sql_side_effect

    apply_column_comments(spark, full_table_name, var_labels)

    # All three attempted, but one raises and is handled
    assert spark.sql.call_count == 3
    out = capsys.readouterr().out
    assert "Skipping comment for schema.tbl.col2" in out


def test_get_table_version_uses_describe_history_and_returns_max_version(monkeypatch):
    spark = MagicMock()

    hist = MagicMock()
    sel = MagicMock()
    ord_df = MagicMock()

    ord_df.first.return_value = {"version": 17}

    hist.select.return_value = sel
    sel.orderBy.return_value = ord_df
    spark.sql.return_value = hist

    class FakeCol:
        def __init__(self, name):
            self.name = name
        def desc(self):
            return self

    monkeypatch.setattr(
        "helpers.ingestion_pipeline.col",
        lambda name: FakeCol(name),
    )

    v = get_table_version(spark, "schema.tbl")

    spark.sql.assert_called_once()
    assert "DESCRIBE HISTORY schema.tbl" in spark.sql.call_args[0][0]
    assert v == 17



def test_table_exists_delegates_to_spark_catalog():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    assert table_exists(spark, "schema.tbl") is True
    spark.catalog.tableExists.assert_called_once_with("schema.tbl")
