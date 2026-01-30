import re
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pandas as pd
import pytest

from helpers.ingestion_pipeline import *


def _normalize_sql(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

@pytest.mark.parametrize(
    "harm_type,household_level,table_version,expected_fragments,unexpected_fragments",
    [
        (
            None, None, None,
            [
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
        (
            "GLD", True, 12,
            [
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
        (
            "GLD-Light", False, 0,
            [
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



def test_compute_stacking_panel_always_zero():
    pdf = pd.DataFrame(
        {
            "table_name": ["my_panel_table", "another_panel"],
            "quarter": [None, "Q1"],
            "country": ["USA", "USA"],
            "year": [2020, 2020],
            "harmonization": ["GLD", "GLD"],
            "M_version": [1, 1],
            "A_version": [1, 1],
        }
    )
    out = compute_stacking(pdf)
    assert (out["stacking"] == 0).all()


def test_compute_stacking_no_conflicts_defaults_to_one_except_panel():
    pdf = pd.DataFrame(
        {
            "table_name": ["tbl_a", "tbl_b", "tbl_panel"],
            "quarter": [None, "Q1", None],
            "country": ["USA", "USA", "USA"],
            "year": [2020, 2020, 2020],
            "harmonization": ["GLD", "GLD", "GLD"],
            "M_version": [1, 1, 1],
            "A_version": [1, 1, 1],
        }
    )
    out = compute_stacking(pdf)
    assert out.loc[0, "stacking"] == 1  
    assert out.loc[1, "stacking"] == 1  
    assert out.loc[2, "stacking"] == 0 

def test_compute_stacking_conflict_resolution_picks_best_annual_row():
    pdf = pd.DataFrame(
        {
            "table_name": ["tbl_1", "tbl_2", "tbl_3"],
            "quarter": [None, None, None],
            "country": ["USA", "USA", "CAN"],
            "year": [2020, 2020, 2020],
            "harmonization": ["GLD-Light", "GLD", "GLD-Light"],
            "M_version": [1, 1, 1],
            "A_version": [1, 1, 1],
        }
    )
    out = compute_stacking(pdf)

    # For USA 2020: GLD beats GLD-Light
    usa_rows = out[(out["country"] == "USA") & (out["year"] == 2020)]
    assert usa_rows["stacking"].sum() == 1
    assert int(usa_rows.loc[usa_rows["harmonization"] == "GLD", "stacking"].iloc[0]) == 1
    assert int(usa_rows.loc[usa_rows["harmonization"] == "GLD-Light", "stacking"].iloc[0]) == 0

    # CAN 2020 has no duplicate, stays 1
    can_row = out[(out["country"] == "CAN") & (out["year"] == 2020)].iloc[0]
    assert int(can_row["stacking"]) == 1


def test_compute_stacking_conflict_tiebreaks_by_M_then_A_versions():
    pdf = pd.DataFrame(
        {
            "table_name": ["tbl_1", "tbl_2", "tbl_3"],
            "quarter": [None, None, None],
            "country": ["USA", "USA", "USA"],
            "year": [2020, 2020, 2020],
            "harmonization": ["GLD", "GLD", "GLD"],
            "M_version": [1, 2, 2],
            "A_version": [5, 1, 9],
        }
    )
    out = compute_stacking(pdf)
    usa = out[(out["country"] == "USA") & (out["year"] == 2020)].copy()

    assert usa["stacking"].sum() == 1
    winner = usa[usa["stacking"] == 1].iloc[0]
    assert int(winner["M_version"]) == 2
    assert int(winner["A_version"]) == 9



def test_compute_stacking_annual_conflict_does_not_change_quarterly_rows():
    pdf = pd.DataFrame(
        {
            "table_name": ["tbl_annual_light", "tbl_annual_gld", "tbl_quarterly"],
            "quarter": [None, "", "Q1"], 
            "country": ["USA", "USA", "USA"],
            "year": [2020, 2020, 2020],
            "harmonization": ["GLD-Light", "GLD", "GLD-Light"],
            "M_version": [1, 1, 1],
            "A_version": [1, 1, 1],
        }
    )

    out = compute_stacking(pdf)

    quarterly = out[out["quarter"].astype(str).str.upper().str.contains("Q")].iloc[0]
    assert int(quarterly["stacking"]) == 1
    annual = out[out["quarter"].isna() | (out["quarter"].astype(str).str.strip() == "")]
    usa_annual = annual[(annual["country"] == "USA") & (annual["year"] == 2020)]
    assert usa_annual["stacking"].sum() == 1
    assert int(usa_annual.loc[usa_annual["harmonization"] == "GLD", "stacking"].iloc[0]) == 1
    assert int(usa_annual.loc[usa_annual["harmonization"] == "GLD-Light", "stacking"].iloc[0]) == 0




def test_get_table_version_uses_describe_history_and_returns_max_version(monkeypatch):
    spark = MagicMock()

    hist = MagicMock()
    sel = MagicMock()
    ord_df = MagicMock()

    ord_df.first.return_value = {"version": 17}

    hist.select.return_value = sel
    sel.orderBy.return_value = ord_df

    spark.sql.return_value = hist

    v = get_table_version(spark, "schema.tbl")

    spark.sql.assert_called_once()
    assert "DESCRIBE HISTORY schema.tbl" in spark.sql.call_args[0][0]
    assert v == 17



def test_table_exists_delegates_to_spark_catalog():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    assert table_exists(spark, "schema.tbl") is True
    spark.catalog.tableExists.assert_called_once_with("schema.tbl")
