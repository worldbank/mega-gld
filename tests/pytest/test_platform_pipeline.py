import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from helpers.platform_pipeline import *


def _triple(depth):
    return depth["likely_4digit"], depth["likely_3digit"], depth["likely_2digit"]


# ------------------------------------------------------------------------------
# Pure helpers
# ------------------------------------------------------------------------------

def test_build_isco_reference_sets():
    d4, d3, d2 = build_isco_reference_sets(["1111", " 2211 ", "0110", "11", None])
    assert d4 == {"1111", "2211", "0110"}
    assert d3 == {"111", "221", "011"}
    assert d2 == {"11", "22", "01"}


@pytest.mark.parametrize("raw,expected", [
    ("ISCO-08", "ISCO-08"), ("isco 2008", "ISCO-08"),
    ("ISCO_88", "ISCO-88"), ("1988", "ISCO-88"),
    ("ISCO-68", None), (None, None),
])
def test_normalize_isco_version(raw, expected):
    assert normalize_isco_version(raw) == expected


def test_survey_key():
    key = survey_key({"country": "THA", "year": 2021, "survey": "LFS", "quarter": " Q2 "})
    assert key == {"country": "THA", "survey_year": "2021", "survey": "LFS", "quarter": "Q2"}
    assert survey_key({"country": "THA", "year": 2021, "survey": "LFS", "quarter": None})["quarter"] == ""


def test_add_composite_flags():
    out = add_composite_flags({"contract": "", "whours": "X", WAGE_VARIABLE: ""})
    assert (out["informality"], out["wage_available"]) == ("X", "")
    out = add_composite_flags({WAGE_VARIABLE: "X"})
    assert (out["informality"], out["wage_available"]) == ("", "X")


# ------------------------------------------------------------------------------
# ISCO classification (one case per decision path)
# ------------------------------------------------------------------------------

def _isco_stats(n4=0, n3=0, n2=0, unknown=0, last0=0, last00=0):
    total = n4 + n3 + n2 + unknown
    return {
        "n_total": total, "n_4digit": n4, "n_3digit": n3, "n_2digit": n2,
        "n_unknown": unknown, "n_non_af": total, "n_last0": last0, "n_last00": last00,
    }


@pytest.mark.parametrize("stats,expected", [
    (_isco_stats(n4=90, n3=10, last0=10),             ("Yes", "No", "No")),  # genuine 4-digit
    (_isco_stats(n4=100, last0=100, last00=5),        ("No", "Yes", "No")),  # padding -> 3-digit
    (_isco_stats(n4=100, last0=100, last00=90),       ("No", "No", "Yes")),  # padding -> 2-digit
    (_isco_stats(n4=10, n3=90, last0=20),             ("No", "Yes", "No")),  # 3-digit match
    (_isco_stats(n3=10, n2=90, last0=20),             ("No", "No", "Yes")),  # 2-digit match
    (_isco_stats(n4=40, unknown=60, last0=5),         ("Yes", "No", "No")),  # unknowns don't dilute
    (_isco_stats(n4=50, n3=50, last0=80, last00=10),  ("No", "Yes", "No")),  # inconclusive -> trailing zero
])
def test_classify_isco_reference(stats, expected):
    depth, _ = classify_isco_reference(stats)
    assert _triple(depth) == expected


@pytest.mark.parametrize("pct_last0,pct_last00,expected", [
    (10, 0,  ("Yes", "No", "No")),
    (90, 10, ("No", "Yes", "No")),
    (95, 90, ("No", "No", "Yes")),
])
def test_classify_trailing_zero_depth(pct_last0, pct_last00, expected):
    assert _triple(classify_trailing_zero_depth(pct_last0, pct_last00)) == expected


# ------------------------------------------------------------------------------
# ISIC classification
# ------------------------------------------------------------------------------

def _isic_stats(section=0, alphanum=0, numeric=0, other=0, last0=0, last00=0):
    return {
        "n_total": section + alphanum + numeric + other,
        "n_section": section, "n_alphanum": alphanum, "n_numeric": numeric,
        "n_last0": last0, "n_last00": last00,
    }


@pytest.mark.parametrize("stats,section,depth,unrecognized", [
    (_isic_stats(section=60, numeric=40),             "Yes", ("No", "No", "No"),   ""),
    (_isic_stats(section=30, alphanum=30, numeric=40), "Yes", ("No", "No", "No"),  ""),  # shares combined
    (_isic_stats(numeric=100, last0=10),              "No",  ("Yes", "No", "No"),  ""),
    (_isic_stats(numeric=100, last0=100, last00=95),  "No",  ("No", "No", "Yes"),  ""),
    (_isic_stats(other=10),                           "",    ("", "", ""),         "X"),
])
def test_classify_isic(stats, section, depth, unrecognized):
    out = classify_isic(stats)
    assert out["present"] == "X"
    assert out["isic_likely_section"] == section
    assert _triple(out) == depth
    assert out["isic_format_unrecognized"] == unrecognized


# ------------------------------------------------------------------------------
# Spark helpers (skipped when pyspark is not installed, e.g. locally)
# ------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    return pyspark_sql.SparkSession.builder.getOrCreate()


def test_get_presence_flags_ignores_blank_strings_and_nan(spark):
    df = spark.createDataFrame(
        [("", float("nan"), "a"), ("  ", None, None)],
        "survname string, wage_no_compen double, contract string",
    )
    flags = get_presence_flags(df, df.columns, ["survname", "wage_no_compen", "contract", "urban"])
    assert flags == {"survname": "", "wage_no_compen": "", "contract": "X", "urban": ""}


def test_compute_isco_depth(spark):
    # Reference matching keeps leading zeros
    refs = {"ISCO-08": build_isco_reference_sets(["1111", "2211", "0110"])}
    df = spark.createDataFrame([("1111",), ("2211",), ("0110",)], "occup_isco string")
    out = compute_isco_depth(df, df.columns, "ISCO-08", refs)
    assert (out["isco_present"], out["isco_likely_4digit"], out["isco_2_present"]) == ("X", "Yes", "")

    # Float and unpadded codes are normalized before the trailing-zero check
    df = spark.createDataFrame([(1100.0,), (22.0,), (3300.0,)], "occup_isco double")
    out = compute_isco_depth(df, df.columns, "ISCO-68", {})
    assert (out["isco_likely_4digit"], out["isco_likely_3digit"], out["isco_likely_2digit"]) == ("No", "No", "Yes")


def test_compute_isic_depth(spark):
    df = spark.createDataFrame([(1400.0,), (22.0,)], "industrycat_isic double")
    out = compute_isic_depth(df, df.columns)
    assert (out["isic_format_unrecognized"], out["isic_likely_2digit"]) == ("", "Yes")

    df = spark.createDataFrame([("a",), ("C",), ("1410",)], "industrycat_isic string")
    assert compute_isic_depth(df, df.columns)["isic_likely_section"] == "Yes"
