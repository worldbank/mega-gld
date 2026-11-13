import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from helpers.platform_pipeline import *


def _triple(depth):
    return depth["likely_4digit"], depth["likely_3digit"], depth["likely_2digit"]


# ------------------------------------------------------------------------------
# ISCO reference matching
# ------------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("ISCO-08", "ISCO-08"), ("isco 2008", "ISCO-08"),
    ("ISCO_88", "ISCO-88"), ("1988", "ISCO-88"),
    ("ISCO-68", None), ("", None),
])
def test_normalize_isco_version(raw, expected):
    assert normalize_isco_version(raw) == expected


@pytest.mark.parametrize("code,expected", [
    ("1111", "4digit"), (" 111 ", "3digit"), ("11", "2digit"), ("9999", "unknown"), (None, None),
])
def test_classify_isco_code(code, expected):
    assert classify_isco_code(code, {"1111"}, {"111"}, {"11"}) == expected


# ------------------------------------------------------------------------------
# ISCO depth rules (one case per decision path)
# ------------------------------------------------------------------------------

def _isco_stats(n4=0, n3=0, n2=0, unknown=0, last0=0, last00=0):
    total = n4 + n3 + n2 + unknown
    return {
        "n_total": total, "n_4digit": n4, "n_3digit": n3, "n_2digit": n2,
        "n_unknown": unknown, "n_non_af": total, "n_last0": last0, "n_last00": last00,
    }


@pytest.mark.parametrize("stats,expected", [
    (_isco_stats(n4=90, n3=10, last0=10),            ("Yes", "No", "No")),  # genuine 4-digit
    (_isco_stats(n4=100, last0=100, last00=5),       ("No", "Yes", "No")),  # padding -> 3-digit
    (_isco_stats(n4=100, last0=100, last00=90),      ("No", "No", "Yes")),  # padding -> 2-digit
    (_isco_stats(n4=10, n3=90, last0=20),            ("No", "Yes", "No")),  # 3-digit match
    (_isco_stats(n3=10, n2=90, last0=20),            ("No", "No", "Yes")),  # 2-digit match
    (_isco_stats(n4=40, unknown=60, last0=5),        ("Yes", "No", "No")),  # unknowns don't dilute
    (_isco_stats(n4=50, n3=50, last0=80, last00=10), ("No", "Yes", "No")),  # inconclusive -> trailing zero
    (_isco_stats(),                                  ("", "", "")),         # no codes
])
def test_classify_isco_reference(stats, expected):
    depth, _ = classify_isco_reference(stats)
    assert _triple(depth) == expected


def test_classify_isco_reference_reports_unknown_share():
    _, pct_unknown = classify_isco_reference(_isco_stats(n4=40, unknown=60))
    assert pct_unknown == 60


@pytest.mark.parametrize("last0,last00,expected", [
    (10, 0,  ("Yes", "No", "No")),
    (90, 10, ("No", "Yes", "No")),
    (95, 90, ("No", "No", "Yes")),
])
def test_classify_isco_trailing_zero(last0, last00, expected):
    stats = {"n_non_af": 100, "n_last0": last0, "n_last00": last00}
    assert _triple(classify_isco_trailing_zero(stats)) == expected


# ------------------------------------------------------------------------------
# ISIC depth rules
# ------------------------------------------------------------------------------

def _isic_stats(section=0, alphanum=0, numeric=0, other=0, last0=0, last00=0):
    return {
        "n_total": section + alphanum + numeric + other,
        "n_section": section, "n_alphanum": alphanum, "n_numeric": numeric,
        "n_last0": last0, "n_last00": last00,
    }


@pytest.mark.parametrize("stats,section,depth,unrecognized", [
    (_isic_stats(section=60, numeric=40),            "Yes", ("No", "No", "No"),  ""),   # section letters
    (_isic_stats(alphanum=80, numeric=20),           "Yes", ("No", "No", "No"),  ""),   # letter-prefixed
    (_isic_stats(numeric=100, last0=10),             "No",  ("Yes", "No", "No"), ""),   # 4-digit
    (_isic_stats(numeric=100, last0=100, last00=95), "No",  ("No", "No", "Yes"), ""),   # 2-digit
    (_isic_stats(other=10),                          "",    ("", "", ""),        "X"),  # unrecognized
])
def test_classify_isic(stats, section, depth, unrecognized):
    out = classify_isic(stats)
    assert out["present"] == "X"
    assert out["isic_likely_section"] == section
    assert _triple(out) == depth
    assert out["isic_format_unrecognized"] == unrecognized


def test_classify_isic_empty():
    assert classify_isic({"n_total": 0})["present"] == ""


# ------------------------------------------------------------------------------
# Spark helpers (skipped when pyspark is not installed, e.g. locally)
# ------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    return pyspark_sql.SparkSession.builder.getOrCreate()


def test_get_presence_flags(spark):
    df = spark.createDataFrame([(1, None), (2, None)], "age int, male int")
    assert get_presence_flags(df, df.columns, ["age", "male", "urban"]) == {"age": "X", "male": "", "urban": ""}


def test_compute_isco_depth(spark):
    refs = {"ISCO-08": {
        "4digit": {"1111", "2211"}, "3digit": {"111", "221"}, "2digit": {"11", "22"},
        "udf": make_isco_classifier_udf({"1111", "2211"}, {"111", "221"}, {"11", "22"}),
    }}
    df = spark.createDataFrame([("1111",), ("2211",)], "occup_isco string")
    out = compute_isco_depth(df, df.columns, "ISCO-08", refs)
    assert (out["isco_present"], out["isco_likely_4digit"], out["isco_2_present"]) == ("X", "Yes", "")


def test_compute_isic_depth(spark):
    df = spark.createDataFrame([("a",), ("C",), ("1410",)], "industrycat_isic string")
    out = compute_isic_depth(df, df.columns)
    assert (out["isic_present"], out["isic_likely_section"]) == ("X", "Yes")
