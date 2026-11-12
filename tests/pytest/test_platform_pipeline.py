import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from helpers.platform_pipeline import *


# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------

def test_vars_to_check_is_deduplicated_and_complete():
    assert len(VARS_TO_CHECK) == len(set(VARS_TO_CHECK))
    for v in ALL_CHECKVARS + INFORMALITY_PROXIES + [WAGE_VARIABLE]:
        assert v in VARS_TO_CHECK


def test_second_job_12m_uses_schema_wage_unit_name():
    assert "unitwage_2_year" in VARIABLE_GROUPS["second_job_12m"]
    assert "unitwage_total_2_year" not in VARS_TO_CHECK


# ------------------------------------------------------------------------------
# Output key
# ------------------------------------------------------------------------------

@pytest.mark.parametrize("quarter,expected", [(None, ""), ("", ""), (" Q2 ", "Q2"), (3, "3")])
def test_survey_key(quarter, expected):
    key = survey_key({"country": "THA", "year": 2021, "survey": "LFS", "quarter": quarter})
    assert key == {"country": "THA", "survey_year": "2021", "survey": "LFS", "quarter": expected}
    assert list(key) == KEY_COLUMNS


# ------------------------------------------------------------------------------
# ISCO reference loading
# ------------------------------------------------------------------------------

def test_build_isco_reference_sets_keeps_only_4digit_and_derives_prefixes():
    d4, d3, d2 = build_isco_reference_sets(["1111", " 2211 ", "0110", "11", None, float("nan")])
    assert d4 == {"1111", "2211", "0110"}
    assert d3 == {"111", "221", "011"}
    assert d2 == {"11", "22", "01"}


def test_load_isco_reference_normalizes_column_names(monkeypatch):
    def fake_read_excel(path, sheet_name, dtype):
        assert (path, sheet_name) == ("ref.xlsx", "ISCO_08")
        return pd.DataFrame({" Unit ": ["1111", None, "2211"]})

    monkeypatch.setattr(pd, "read_excel", fake_read_excel)
    d4, d3, d2 = load_isco_reference("ref.xlsx", "ISCO_08")
    assert d4 == {"1111", "2211"}


@pytest.mark.parametrize("raw,expected", [
    ("ISCO-08", "ISCO-08"), ("isco 2008", "ISCO-08"), ("08", "ISCO-08"),
    ("ISCO_88", "ISCO-88"), ("1988", "ISCO-88"),
    ("ISCO-68", None), ("", None), (None, None),
])
def test_normalize_isco_version(raw, expected):
    assert normalize_isco_version(raw) == expected


# ------------------------------------------------------------------------------
# Trailing-zero logic
# ------------------------------------------------------------------------------

@pytest.mark.parametrize("pct_last0,pct_last00,expected", [
    (10, 0,  ("Yes", "No", "No")),   # few trailing zeros -> 4-digit
    (90, 10, ("No", "Yes", "No")),   # "0" padding only -> 3-digit
    (95, 90, ("No", "No", "Yes")),   # "00" padding -> 2-digit
])
def test_classify_trailing_zero_depth(pct_last0, pct_last00, expected):
    out = classify_trailing_zero_depth(pct_last0, pct_last00)
    assert (out["likely_4digit"], out["likely_3digit"], out["likely_2digit"]) == expected


def test_classify_isco_trailing_zero_handles_no_non_af_codes():
    out = classify_isco_trailing_zero({"n_non_af": 0, "n_last0": 0, "n_last00": 0})
    assert out == {"likely_4digit": "Yes", "likely_3digit": "No", "likely_2digit": "No"}


# ------------------------------------------------------------------------------
# ISCO reference-based classification
# ------------------------------------------------------------------------------

def _isco_stats(n4=0, n3=0, n2=0, unknown=0, non_af=None, last0=0, last00=0):
    total = n4 + n3 + n2 + unknown
    return {
        "n_total": total, "n_4digit": n4, "n_3digit": n3, "n_2digit": n2,
        "n_unknown": unknown, "n_non_af": total if non_af is None else non_af,
        "n_last0": last0, "n_last00": last00,
    }


def _triple(depth):
    return depth["likely_4digit"], depth["likely_3digit"], depth["likely_2digit"]


def test_classify_isco_reference_genuine_4digit():
    depth, pct_unknown = classify_isco_reference(_isco_stats(n4=90, n3=10, last0=10))
    assert _triple(depth) == ("Yes", "No", "No")
    assert pct_unknown == 0


def test_classify_isco_reference_spurious_padding_to_3digit():
    # Every code matches a 4-digit reference code but all end in "0"
    depth, _ = classify_isco_reference(_isco_stats(n4=100, last0=100, last00=5))
    assert _triple(depth) == ("No", "Yes", "No")


def test_classify_isco_reference_spurious_padding_to_2digit():
    depth, _ = classify_isco_reference(_isco_stats(n4=100, last0=100, last00=90))
    assert _triple(depth) == ("No", "No", "Yes")


def test_classify_isco_reference_3digit_match():
    depth, _ = classify_isco_reference(_isco_stats(n4=10, n3=90, last0=20))
    assert _triple(depth) == ("No", "Yes", "No")


def test_classify_isco_reference_2digit_match():
    depth, _ = classify_isco_reference(_isco_stats(n3=10, n2=90, last0=20))
    assert _triple(depth) == ("No", "No", "Yes")


def test_classify_isco_reference_unknowns_do_not_dilute_and_are_reported():
    depth, pct_unknown = classify_isco_reference(_isco_stats(n4=40, unknown=60, last0=5))
    assert _triple(depth) == ("Yes", "No", "No")
    assert pct_unknown == 60


def test_classify_isco_reference_inconclusive_falls_back_to_trailing_zero():
    depth, _ = classify_isco_reference(_isco_stats(n4=50, n3=50, last0=80, last00=10))
    assert _triple(depth) == ("No", "Yes", "No")


def test_classify_isco_reference_no_matches_falls_back_to_trailing_zero():
    depth, pct_unknown = classify_isco_reference(_isco_stats(unknown=10, last0=0))
    assert _triple(depth) == ("Yes", "No", "No")
    assert pct_unknown == 100


def test_classify_isco_reference_empty():
    depth, pct_unknown = classify_isco_reference(_isco_stats())
    assert _triple(depth) == ("", "", "")
    assert pct_unknown == 0


# ------------------------------------------------------------------------------
# ISIC classification
# ------------------------------------------------------------------------------

def _isic_stats(section=0, alphanum=0, numeric=0, other=0, last0=0, last00=0):
    return {
        "n_total": section + alphanum + numeric + other,
        "n_section": section, "n_alphanum": alphanum, "n_numeric": numeric,
        "n_last0": last0, "n_last00": last00,
    }


def test_classify_isic_empty():
    out = classify_isic({"n_total": 0})
    assert out["present"] == ""
    assert out["isic_format_unrecognized"] == ""


@pytest.mark.parametrize("stats", [_isic_stats(section=60, numeric=40), _isic_stats(alphanum=80, numeric=20)])
def test_classify_isic_section_codes(stats):
    out = classify_isic(stats)
    assert out["present"] == "X"
    assert out["isic_section_present"] == "X"
    assert out["isic_likely_section"] == "Yes"
    assert _triple(out) == ("No", "No", "No")


def test_classify_isic_numeric_4digit():
    out = classify_isic(_isic_stats(numeric=100, last0=10))
    assert out["isic_likely_section"] == "No"
    assert out["isic_section_present"] == ""
    assert _triple(out) == ("Yes", "No", "No")
    assert out["isic_format_unrecognized"] == ""


def test_classify_isic_numeric_2digit():
    out = classify_isic(_isic_stats(numeric=100, last0=100, last00=95))
    assert _triple(out) == ("No", "No", "Yes")


def test_classify_isic_mixed_minority_section_codes_flagged():
    out = classify_isic(_isic_stats(section=10, numeric=90, last0=5))
    assert out["isic_section_present"] == "X"
    assert out["isic_likely_section"] == "No"


def test_classify_isic_unrecognized_format():
    out = classify_isic(_isic_stats(other=10))
    assert out["present"] == "X"
    assert out["isic_format_unrecognized"] == "X"


# ------------------------------------------------------------------------------
# Composite flags
# ------------------------------------------------------------------------------

def test_add_composite_flags():
    out = add_composite_flags({"contract": "", "whours": "X", WAGE_VARIABLE: ""})
    assert out["informality"] == "X"
    assert out["wage_available"] == ""

    out = add_composite_flags({v: "" for v in INFORMALITY_PROXIES} | {WAGE_VARIABLE: "X"})
    assert out["informality"] == ""
    assert out["wage_available"] == "X"


def test_add_composite_flags_does_not_mutate_input():
    presence = {"contract": "X"}
    add_composite_flags(presence)
    assert presence == {"contract": "X"}


# ------------------------------------------------------------------------------
# Spark helpers (skipped when pyspark is not installed, e.g. locally)
# ------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    return pyspark_sql.SparkSession.builder.getOrCreate()


def test_get_presence_flags(spark):
    df = spark.createDataFrame([(1, None), (2, None)], "age int, male int")
    flags = get_presence_flags(df, df.columns, ["age", "male", "urban"])
    assert flags == {"age": "X", "male": "", "urban": ""}


def test_get_presence_flags_ignores_blank_strings_and_nan(spark):
    df = spark.createDataFrame(
        [("", float("nan"), "a"), ("  ", None, None)],
        "survname string, wage_no_compen double, contract string",
    )
    flags = get_presence_flags(df, df.columns, ["survname", "wage_no_compen", "contract"])
    assert flags == {"survname": "", "wage_no_compen": "", "contract": "X"}


def test_get_modal_value(spark):
    df = spark.createDataFrame([("ISCO-08",), ("ISCO-08",), ("ISCO-88",), ("",), (None,)], "isco_version string")
    assert get_modal_value(df, df.columns, "isco_version") == "ISCO-08"
    assert get_modal_value(df, df.columns, "missing") == ""


def test_compute_isco_depth_with_reference(spark):
    refs = {"ISCO-08": build_isco_reference_sets(["1111", "1112", "2211", "0110"])}
    df = spark.createDataFrame([("1111",), ("1112",), ("2211",), ("0110",)], "occup_isco string")
    out = compute_isco_depth(df, df.columns, "ISCO-08", refs)
    assert out["isco_present"] == "X"
    assert (out["isco_likely_4digit"], out["isco_likely_3digit"], out["isco_likely_2digit"]) == ("Yes", "No", "No")
    assert out["isco_2_present"] == ""


def test_compute_isco_depth_without_reference_uses_trailing_zero(spark):
    df = spark.createDataFrame([("1100",), ("2200",), ("3300",)], "occup_isco string")
    out = compute_isco_depth(df, df.columns, "ISCO-68", {})
    assert (out["isco_likely_4digit"], out["isco_likely_3digit"], out["isco_likely_2digit"]) == ("No", "No", "Yes")


def test_compute_isic_depth(spark):
    df = spark.createDataFrame([("a",), ("C",), ("G",), ("1410",)], "industrycat_isic string")
    out = compute_isic_depth(df, df.columns)
    assert out["isic_present"] == "X"
    assert out["isic_likely_section"] == "Yes"

    out = compute_isic_depth(df, ["other"])
    assert out["isic_present"] == ""
