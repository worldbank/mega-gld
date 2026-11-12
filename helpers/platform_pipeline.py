import pandas as pd

try:
    from pyspark.sql import functions as F
except ImportError:  # allows the pure-Python helpers to be tested without Spark
    F = None

# ==============================================================================
# THRESHOLDS
# ==============================================================================

# ISCO/ISIC depth — reference-based matching against official code lists
# A depth level is assigned when the share of matching codes meets the threshold
THRESHOLD_4DIGIT = 75
THRESHOLD_3DIGIT = 75
THRESHOLD_2DIGIT = 80

# ISIC section threshold (special case) - >= 50% of codes are single letters (A-U)
THRESHOLD_SECTION = 50

# Share of non-Armed-Forces codes ending in "0" above which a 4-digit reference
# match is treated as spurious padding of shorter codes
THRESHOLD_SPURIOUS_PADDING = 95

# Share of codes not found in the reference above which a warning is printed
THRESHOLD_UNKNOWN_WARNING = 30

# ==============================================================================
# VARIABLE GROUPS
# ==============================================================================

VARIABLE_GROUPS = {
    "geographic_breakdown": [
        "urban", "subnatid1", "subnatid2", "subnatid3", "subnatid4",
        "subnatidsurvey", "supid"
    ],
    "demographics": [
        "hsize", "age", "male",
        "eye_dsablty", "hear_dsablty", "walk_dsablty",
        "conc_dsord", "slfcre_dsablty", "comm_dsablty"
    ],
    "education": [
        "educat7", "educat5", "educat4"
    ],
    "vocational_training": [
        "vocational", "vocational_type", "vocational_length_l", "vocational_financed"
    ],
    "labor_status": [
        "lstatus", "underemployment", "nlfreason", "empstat",
        "occup", "industrycat10",
        "wage_no_compen", "whours", "unitwage",
        "contract", "healthins", "socialsec",
        "firmsize_l", "firmsize_u"
    ],
    "second_job": [
        "empstat_2", "occup_2", "industrycat10_2",
        "wage_total_2", "whours_2", "unitwage_2",
        "firmsize_l_2", "firmsize_u_2"
    ],
    "labor_status_12m": [
        "lstatus_year", "underemployment_year", "nlfreason_year",
        "unempldur_l_year", "unempldur_u_year",
        "empstat_year", "occup_year", "industrycat10_year",
        "wage_total_year", "whours_year", "unitwage_year",
        "contract_year", "healthins_year", "socialsec_year"
    ],
    "second_job_12m": [
        "empstat_2_year", "industrycat10_2_year", "occup_2_year",
        "wage_total_2_year", "whours_2_year", "unitwage_2_year",
        "t_wage_total_year"
    ],
    "migration": [
        "migrated_binary", "migrated_years", "migrated_from_urban",
        "migrated_from_cat", "migrated_from_country", "migrated_reason"
    ],
    "occupation_industry_codes": [
        "occup_isco", "occup_isco_2", "occup_isco_year", "occup_isco_2_year",
        "industrycat_isic", "industrycat_isic_2",
        "industrycat_isic_year", "industrycat_isic_2_year"
    ]
}

ALL_CHECKVARS = [v for group in VARIABLE_GROUPS.values() for v in group]
INFORMALITY_PROXIES = ["contract", "healthins", "socialsec", "whours", "unitwage"]
WAGE_VARIABLE = "wage_no_compen"

# Every variable whose presence is checked, deduplicated and in a stable order.
# Computed once here instead of on every survey iteration.
VARS_TO_CHECK = list(dict.fromkeys(ALL_CHECKVARS + INFORMALITY_PROXIES + [WAGE_VARIABLE]))


# ==============================================================================
# PURE HELPERS (no Spark)
# ==============================================================================

def build_isco_reference_sets(codes):
    """
    Builds the three reference sets for one ISCO version from its unit group codes.
    Returns (valid_4digit, valid_3digit, valid_2digit).
    """
    cleaned = (str(c).strip() for c in codes if c is not None and not pd.isna(c))
    valid_4digit = {c for c in cleaned if len(c) == 4}
    valid_3digit = {c[:3] for c in valid_4digit}
    valid_2digit = {c[:2] for c in valid_4digit}
    return valid_4digit, valid_3digit, valid_2digit


def load_isco_reference(excel_path, sheet_name):
    """
    Loads unit group codes from a sheet in the ILO ISCO Excel file
    (the "unit" column) and returns the three reference sets.
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    return build_isco_reference_sets(df["unit"].dropna())


def normalize_isco_version(isco_ver):
    """
    Maps the free-text isco_version value to "ISCO-08", "ISCO-88" or None
    (None means no 4-digit reference is available for that version).
    """
    norm = str(isco_ver).strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    if norm in {"isco2008", "isco08", "2008", "08"}:
        return "ISCO-08"
    if norm in {"isco1988", "isco88", "1988", "88"}:
        return "ISCO-88"
    return None


def _depth(d4, d3, d2):
    return {"likely_4digit": d4, "likely_3digit": d3, "likely_2digit": d2}


def classify_trailing_zero_depth(pct_last0, pct_last00):
    """
    Trailing-zero depth logic: codes padded with "0"/"00" are likely 3-/2-digit.
    """
    return _depth(
        "Yes" if pct_last0 < THRESHOLD_4DIGIT else "No",
        "Yes" if (pct_last0 >= THRESHOLD_3DIGIT and pct_last00 < THRESHOLD_2DIGIT) else "No",
        "Yes" if pct_last00 >= THRESHOLD_2DIGIT else "No",
    )


def _trailing_zero_pcts(stats):
    n_non_af = stats["n_non_af"]
    if n_non_af > 0:
        return 100 * stats["n_last0"] / n_non_af, 100 * stats["n_last00"] / n_non_af
    return 0, 0


def classify_isco_trailing_zero(stats):
    """
    Depth for ISCO versions without a 4-digit reference (e.g. ISCO-68, ISCO-58).
    stats: n_non_af, n_last0, n_last00 (Armed Forces codes already excluded).
    """
    pct_last0, pct_last00 = _trailing_zero_pcts(stats)
    return classify_trailing_zero_depth(pct_last0, pct_last00)


def classify_isco_reference(stats):
    """
    Depth for ISCO-08 / ISCO-88, combining reference matching and trailing zeros.

    stats: n_total, n_4digit, n_3digit, n_2digit, n_unknown,
           n_non_af, n_last0, n_last00.

    Reference percentages are computed over matched codes only so that unknown
    codes do not dilute the signal. If nearly all codes end in "0" while the
    reference says 4-digit, the match is treated as spurious padding.

    Returns (depth_dict, pct_unknown).
    """
    if stats["n_total"] == 0:
        return _depth("", "", ""), 0

    n_matched = stats["n_4digit"] + stats["n_3digit"] + stats["n_2digit"]
    if n_matched > 0:
        pct_4digit = 100 * stats["n_4digit"] / n_matched
        pct_3digit = 100 * stats["n_3digit"] / n_matched
        pct_2digit = 100 * stats["n_2digit"] / n_matched
    else:
        pct_4digit = pct_3digit = pct_2digit = 0

    pct_last0, pct_last00 = _trailing_zero_pcts(stats)
    pct_unknown = 100 * stats["n_unknown"] / stats["n_total"]

    if pct_last0 >= THRESHOLD_SPURIOUS_PADDING and pct_4digit >= THRESHOLD_4DIGIT:
        # Likely spurious padding of shorter codes — decide between 3 and 2 digits
        # (if "00" padding is not dominant, the codes are 3-digit)
        if pct_last00 >= THRESHOLD_2DIGIT:
            depth = _depth("No", "No", "Yes")
        else:
            depth = _depth("No", "Yes", "No")
    elif pct_last00 >= THRESHOLD_2DIGIT:
        depth = _depth("No", "No", "Yes")
    elif pct_4digit >= THRESHOLD_4DIGIT:
        depth = _depth("Yes", "No", "No")
    elif pct_3digit >= THRESHOLD_3DIGIT:
        depth = _depth("No", "Yes", "No")
    elif pct_2digit >= THRESHOLD_2DIGIT:
        depth = _depth("No", "No", "Yes")
    else:
        depth = classify_trailing_zero_depth(pct_last0, pct_last00)

    return depth, pct_unknown


def classify_isic(stats):
    """
    ISIC depth handling three code formats found in GLD surveys:
      1. Pure section letters  (e.g. "A")     : ^[A-U]$
      2. Letter-prefix numeric (e.g. "C1410") : ^[A-U][0-9]+$
      3. Pure numeric          (e.g. "1400")  : ^[0-9]+$  (trailing-zero logic)

    stats: n_total, n_section, n_alphanum, n_numeric, n_last0, n_last00
           (trailing-zero counts over numeric codes only).
    """
    result = {
        "present": "",
        "isic_section_present": "",
        "isic_likely_section": "",
        "likely_2digit": "",
        "likely_3digit": "",
        "likely_4digit": "",
        "isic_format_unrecognized": "",
    }
    n_total = stats["n_total"]
    if n_total == 0:
        return result

    result["present"] = "X"
    n_section, n_alphanum, n_numeric = stats["n_section"], stats["n_alphanum"], stats["n_numeric"]

    if n_section > 0 or n_alphanum > 0:
        result["isic_section_present"] = "X"

    # Both pure letters and letter-prefixed codes carry the section, so their
    # shares are combined
    if 100 * (n_section + n_alphanum) / n_total >= THRESHOLD_SECTION:
        result["isic_likely_section"] = "Yes"
        result.update(_depth("No", "No", "No"))
    elif n_numeric > 0:
        result["isic_likely_section"] = "No"
        result.update(classify_trailing_zero_depth(
            100 * stats["n_last0"] / n_numeric,
            100 * stats["n_last00"] / n_numeric,
        ))
        if all(result[k] == "No" for k in ["likely_2digit", "likely_3digit", "likely_4digit"]):
            result["isic_format_unrecognized"] = "X"
    else:
        result["isic_format_unrecognized"] = "X"

    return result


KEY_COLUMNS = ["country", "survey_year", "survey", "quarter"]


def survey_key(meta_row):
    """
    Builds the output table key (country, survey_year, survey, quarter) from an
    _ingestion_metadata row. Used both for the MERGE and for removing stale rows,
    so both must produce identical values.
    """
    quarter = meta_row["quarter"]
    return {
        "country":     meta_row["country"],
        "survey_year": str(meta_row["year"]),
        "survey":      meta_row["survey"],
        "quarter":     str(quarter).strip() if quarter else "",
    }


def add_composite_flags(presence):
    """Adds the informality and wage_available flags derived from individual variables."""
    presence = dict(presence)
    presence["informality"] = "X" if any(presence.get(v) == "X" for v in INFORMALITY_PROXIES) else ""
    presence["wage_available"] = "X" if presence.get(WAGE_VARIABLE) == "X" else ""
    return presence


# ==============================================================================
# SPARK HELPERS
# ==============================================================================

def _non_empty_codes(df, colname):
    return (
        df.filter(_is_populated(df, colname))
          .filter(F.trim(F.col(colname).cast("string")) != "")
    )


def get_modal_value(df, col_names, varname):
    """
    Returns the modal non-null, non-empty value of a column as a string,
    or "" if the column is missing or fully empty.
    """
    if varname not in col_names:
        return ""
    row = (
        _non_empty_codes(df, varname)
          .groupBy(varname)
          .count()
          .orderBy(F.desc("count"))
          .select(varname)
          .limit(1)
          .collect()
    )
    return str(row[0][0]).strip() if row else ""


def _is_populated(df, colname):
    """
    True when a value is genuinely present: not null, not a blank string
    (Stata missing strings are read as "") and not a floating-point NaN.
    """
    dtype = dict(df.dtypes)[colname]
    col = F.col(colname)
    cond = col.isNotNull()
    if dtype == "string":
        cond = cond & (F.trim(col) != "")
    elif dtype in ("double", "float"):
        cond = cond & ~F.isnan(col)
    return cond


def get_presence_flags(df, col_names, checkvars):
    """
    Checks all variables in a single aggregation query per table.
    Returns "X" if a variable exists and has at least one non-missing value, "" otherwise.
    """
    existing = [v for v in checkvars if v in col_names]
    if not existing:
        return {v: "" for v in checkvars}

    counts = df.agg(*[
        F.sum(F.when(_is_populated(df, v), 1).otherwise(0)).alias(v)
        for v in existing
    ]).collect()[0]

    return {
        v: ("X" if (v in existing and counts[v] and counts[v] > 0) else "")
        for v in checkvars
    }


def _normalized_code(colname):
    """
    Code as a trimmed, upper-case string. Integral floats lose their decimal
    suffix ("1100.0" -> "1100") because ingestion infers column types, so codes
    can arrive as doubles. Leading zeros in string codes are preserved.
    """
    as_str = F.upper(F.trim(F.col(colname).cast("string")))
    return F.regexp_replace(as_str, r"^([0-9]+)\.0+$", "$1")


def _padded_code(code):
    """
    Short numeric codes right-padded to 4 characters with zeros, following the
    GLD convention that less detailed codes are padded ("14" -> "1400"), so the
    trailing-zero heuristic also works for unpadded codes.
    """
    return F.when(
        code.rlike("^[0-9]{1,3}$"), F.rpad(code, 4, "0")
    ).otherwise(code)


def _isco_stats(base, ref_sets):
    """
    Aggregates trailing-zero counts (excluding Armed Forces codes, which start
    with "0") and, when reference sets are given, reference-match counts.
    Reference matching uses the unpadded code and native isin() expressions
    instead of a Python UDF; trailing zeros use the padded code.
    """
    code = F.col("code_str")
    padded = _padded_code(code)
    non_af = ~code.startswith("0")
    aggs = [
        F.count("*").alias("n_total"),
        F.sum(non_af.cast("int")).alias("n_non_af"),
        F.sum((non_af & padded.endswith("0")).cast("int")).alias("n_last0"),
        F.sum((non_af & padded.endswith("00")).cast("int")).alias("n_last00"),
    ]
    if ref_sets is not None:
        ref_4d, ref_3d, ref_2d = (sorted(s) for s in ref_sets)
        match_class = (
            F.when(code.isin(ref_4d), "4digit")
             .when(code.isin(ref_3d), "3digit")
             .when(code.isin(ref_2d), "2digit")
             .otherwise("unknown")
        )
        aggs += [
            F.sum((match_class == cls).cast("int")).alias(f"n_{cls}")
            for cls in ["4digit", "3digit", "2digit", "unknown"]
        ]
    return base.agg(*aggs).collect()[0].asDict()


def compute_isco_depth(df, col_names, isco_ver, isco_refs):
    """
    ISCO depth for main job (occup_isco) and second job (occup_isco_2).

    isco_refs: {"ISCO-08": (4d, 3d, 2d), "ISCO-88": (4d, 3d, 2d)}, loaded once per run.
    ISCO-08/88 use reference matching + trailing zeros; other versions use
    trailing zeros only.
    """
    ref_version = normalize_isco_version(isco_ver)
    ref_sets = isco_refs.get(ref_version) if ref_version else None

    def _depth_for_col(colname):
        result = {"present": "", **_depth("", "", "")}
        if colname not in col_names:
            return result

        base = _non_empty_codes(df, colname).select(_normalized_code(colname).alias("code_str"))
        stats = _isco_stats(base, ref_sets)
        if not stats["n_total"]:
            return result

        result["present"] = "X"
        if ref_sets is not None:
            depth, pct_unknown = classify_isco_reference(stats)
            if pct_unknown > THRESHOLD_UNKNOWN_WARNING:
                print(f"    NOTE: {colname} — {pct_unknown:.0f}% of codes unrecognised in "
                      f"{ref_version} reference. Classification may be unreliable.")
        else:
            depth = classify_isco_trailing_zero(stats)
        result.update(depth)
        return result

    main = _depth_for_col("occup_isco")
    seco = _depth_for_col("occup_isco_2")

    return {
        "isco_present":         main["present"],
        "isco_likely_4digit":   main["likely_4digit"],
        "isco_likely_3digit":   main["likely_3digit"],
        "isco_likely_2digit":   main["likely_2digit"],
        "isco_2_present":       seco["present"],
        "isco_2_likely_4digit": seco["likely_4digit"],
        "isco_2_likely_3digit": seco["likely_3digit"],
        "isco_2_likely_2digit": seco["likely_2digit"],
    }


def compute_isic_depth(df, col_names):
    """ISIC depth for main job (industrycat_isic). See classify_isic."""
    colname = "industrycat_isic"
    if colname not in col_names:
        main = classify_isic({"n_total": 0})
    else:
        code = F.col("code_trim")
        padded = _padded_code(code)
        is_numeric = code.rlike("^[0-9]+$")
        stats = (
            _non_empty_codes(df, colname)
              .select(_normalized_code(colname).alias("code_trim"))
              .agg(
                  F.count("*").alias("n_total"),
                  F.sum(code.rlike("^[A-U]$").cast("int")).alias("n_section"),
                  F.sum(code.rlike("^[A-U][0-9]+$").cast("int")).alias("n_alphanum"),
                  F.sum(is_numeric.cast("int")).alias("n_numeric"),
                  F.sum((is_numeric & padded.endswith("0")).cast("int")).alias("n_last0"),
                  F.sum((is_numeric & padded.endswith("00")).cast("int")).alias("n_last00"),
              )
              .collect()[0]
              .asDict()
        )
        main = classify_isic(stats)

    return {
        "isic_present":             main["present"],
        "isic_section_present":     main["isic_section_present"],
        "isic_likely_section":      main["isic_likely_section"],
        "isic_likely_2digit":       main["likely_2digit"],
        "isic_likely_3digit":       main["likely_3digit"],
        "isic_likely_4digit":       main["likely_4digit"],
        "isic_format_unrecognized": main["isic_format_unrecognized"],
    }
