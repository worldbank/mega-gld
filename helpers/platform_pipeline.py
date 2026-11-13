import pandas as pd

try:
    from pyspark.sql import functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
except ImportError:  # allows the pure-Python helpers to be tested without Spark
    F = udf = StringType = None

# ==============================================================================
# THRESHOLDS
# ==============================================================================

# ISCO/ISIC depth — reference-based matching against official code lists
# A depth level is assigned when the share of matching codes meets the threshold
THRESHOLD_4DIGIT  = 75
THRESHOLD_3DIGIT  = 75
THRESHOLD_2DIGIT  = 80

# ISIC section threshold (special case) - >= 50% of codes are single letters (A-U)
THRESHOLD_SECTION = 50

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
        "wage_total_2_year", "whours_2_year", "unitwage_total_2_year",
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

ALL_CHECKVARS       = [v for group in VARIABLE_GROUPS.values() for v in group]
INFORMALITY_PROXIES = ["contract", "healthins", "socialsec", "whours", "unitwage"]

# All variables checked for presence — computed once instead of on every survey.
# (wage_no_compen is already part of ALL_CHECKVARS)
ALL_VARS_TO_CHECK = ALL_CHECKVARS + [
    v for v in INFORMALITY_PROXIES if v not in ALL_CHECKVARS
]


# ==============================================================================
# ISCO REFERENCE
# ==============================================================================

def load_isco_reference(excel_path, sheet_name):
    """
    Loads unit group codes from a sheet in the ILO ISCO Excel file.
    Returns three sets: valid_4digit, valid_3digit, valid_2digit.
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    codes = df["unit"].dropna().str.strip()
    valid_4digit = {c for c in codes if len(c) == 4}
    valid_3digit = {c[:3] for c in valid_4digit}
    valid_2digit = {c[:2] for c in valid_4digit}
    return valid_4digit, valid_3digit, valid_2digit


def normalize_isco_version(isco_ver):
    """
    Maps isco_version to "ISCO-08" or "ISCO-88" (versions with a 4-digit
    reference), or None for all other versions.
    """
    isco_ver_norm = str(isco_ver).strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    if isco_ver_norm in {"isco2008", "isco08", "2008", "08"}:
        return "ISCO-08"
    elif isco_ver_norm in {"isco1988", "isco88", "1988", "88"}:
        return "ISCO-88"
    return None


def classify_isco_code(code, ref_4digit, ref_3digit, ref_2digit):
    """Classifies a single code against the reference sets."""
    if code is None:
        return None
    code = str(code).strip()
    if code in ref_4digit:
        return "4digit"
    elif code in ref_3digit:
        return "3digit"
    elif code in ref_2digit:
        return "2digit"
    else:
        return "unknown"


def make_isco_classifier_udf(ref_4digit, ref_3digit, ref_2digit):
    """
    Builds the classification UDF for one ISCO version. Called once per
    version per run, not for every column of every survey.
    """
    return udf(lambda code: classify_isco_code(code, ref_4digit, ref_3digit, ref_2digit), StringType())


def build_isco_refs(excel_path):
    """
    Loads ISCO-08 and ISCO-88 reference sets and their classification UDFs once.
    Returns {"ISCO-08": {"4digit", "3digit", "2digit", "udf"}, "ISCO-88": {...}}.
    """
    refs = {}
    for version, sheet in [("ISCO-08", "ISCO_08"), ("ISCO-88", "ISCO_88")]:
        ref_4digit, ref_3digit, ref_2digit = load_isco_reference(excel_path, sheet)
        refs[version] = {
            "4digit": ref_4digit,
            "3digit": ref_3digit,
            "2digit": ref_2digit,
            "udf":    make_isco_classifier_udf(ref_4digit, ref_3digit, ref_2digit),
        }
    return refs


# ==============================================================================
# DEPTH CLASSIFICATION RULES (pure Python, applied to aggregated counts)
# ==============================================================================

def classify_isco_reference(stats):
    """
    ISCO-08 and ISCO-88: combined approach — reference matching + trailing-zero signal.

    Reference matching classifies each code against the official list.
    Percentages are computed over matched codes only (excluding unknowns)
    to avoid dilution from miscoded or non-standard codes.

    Spurious padding detection:
      If >= 95% of non-Armed-Forces codes end in "0" AND reference says
      4-digit, the match is likely spurious padding of 3-digit codes.
      In this case, 3-digit and 2-digit reference is checked instead.

    stats: n_total, n_4digit, n_3digit, n_2digit, n_unknown, n_last0, n_last00, n_non_af
    Returns (result, pct_unknown), result holding likely_4digit/3digit/2digit.
    """
    result = {"likely_4digit": "", "likely_3digit": "", "likely_2digit": ""}
    pct_unknown = 0

    if stats["n_total"] > 0:
        n_matched = stats["n_4digit"] + stats["n_3digit"] + stats["n_2digit"]
        n_non_af  = stats["n_non_af"]

        # Compute reference percentages over matched codes only
        # to avoid unknown codes diluting the signal
        if n_matched > 0:
            pct_4digit = 100 * stats["n_4digit"] / n_matched
            pct_3digit = 100 * stats["n_3digit"] / n_matched
            pct_2digit = 100 * stats["n_2digit"] / n_matched
        else:
            pct_4digit = pct_3digit = pct_2digit = 0

        # Trailing zero percentages over non-Armed-Forces codes only
        if n_non_af > 0:
            pct_last0  = 100 * stats["n_last0"]  / n_non_af
            pct_last00 = 100 * stats["n_last00"] / n_non_af
        else:
            pct_last0 = pct_last00 = 0

        pct_unknown = 100 * stats["n_unknown"] / stats["n_total"]

        # --- Classification logic ---

        if pct_last0 >= 95 and pct_4digit >= THRESHOLD_4DIGIT:
            # Nearly all codes end in zero AND reference says 4-digit
            # — high likelihood of spurious padding of 3-digit codes
            # Check 3-digit and 2-digit reference to distinguish
            if pct_last00 >= THRESHOLD_2DIGIT:
                result["likely_4digit"] = "No"
                result["likely_3digit"] = "No"
                result["likely_2digit"] = "Yes"
            elif pct_3digit >= THRESHOLD_3DIGIT:
                result["likely_4digit"] = "No"
                result["likely_3digit"] = "Yes"
                result["likely_2digit"] = "No"
            else:
                # Reference inconclusive for 3 vs 2 — use trailing zeros
                result["likely_4digit"] = "No"
                result["likely_3digit"] = "Yes" if pct_last00 < THRESHOLD_2DIGIT else "No"
                result["likely_2digit"] = "Yes" if pct_last00 >= THRESHOLD_2DIGIT else "No"

        elif pct_last00 >= THRESHOLD_2DIGIT:
            # Strong double-trailing-zero signal — 2-digit
            result["likely_4digit"] = "No"
            result["likely_3digit"] = "No"
            result["likely_2digit"] = "Yes"

        elif pct_4digit >= THRESHOLD_4DIGIT:
            result["likely_4digit"] = "Yes"
            result["likely_3digit"] = "No"
            result["likely_2digit"] = "No"

        elif pct_3digit >= THRESHOLD_3DIGIT:
            result["likely_4digit"] = "No"
            result["likely_3digit"] = "Yes"
            result["likely_2digit"] = "No"

        elif pct_2digit >= THRESHOLD_2DIGIT:
            result["likely_4digit"] = "No"
            result["likely_3digit"] = "No"
            result["likely_2digit"] = "Yes"

        else:
            # Reference inconclusive — fall back to trailing zero
            result["likely_4digit"] = "Yes" if pct_last0 <  THRESHOLD_4DIGIT else "No"
            result["likely_3digit"] = "Yes" if (pct_last0 >= THRESHOLD_3DIGIT and pct_last00 < THRESHOLD_2DIGIT) else "No"
            result["likely_2digit"] = "Yes" if pct_last00 >= THRESHOLD_2DIGIT else "No"

    return result, pct_unknown


def classify_isco_trailing_zero(stats):
    """
    Trailing-zero only for all other ISCO versions (including ISCO-68 and
    ISCO-58, which have no 4-digit reference). Armed Forces codes are
    excluded from the counts.

    stats: n_non_af, n_last0, n_last00
    """
    n_non_af = stats["n_non_af"]
    if n_non_af > 0:
        pct_last0  = 100 * stats["n_last0"]  / n_non_af
        pct_last00 = 100 * stats["n_last00"] / n_non_af
    else:
        pct_last0 = pct_last00 = 0

    return {
        "likely_4digit": "Yes" if pct_last0  <  THRESHOLD_4DIGIT else "No",
        "likely_3digit": "Yes" if (pct_last0 >= THRESHOLD_3DIGIT and pct_last00 < THRESHOLD_2DIGIT) else "No",
        "likely_2digit": "Yes" if pct_last00 >= THRESHOLD_2DIGIT else "No",
    }


def classify_isic(stats):
    """
    ISIC depth handling three code formats found in GLD surveys:

      1. Pure section letters  (e.g. "A", "C", "G")    : regex ^[A-U]$
      2. Letter-prefix numeric (e.g. "A342", "C1410")   : regex ^[A-U][0-9]+$
      3. Pure numeric          (e.g. "1400", "3410")    : regex ^[0-9]+$
         Numeric codes follow trailing-zero convention.

    For formats 1 and 2, isic_likely_section = "Yes".
    For format 3, trailing-zero logic is applied.

    stats: n_total, n_section, n_alphanum, n_numeric, n_last0, n_last00
    """
    result = {
        "present":                  "",
        "isic_section_present":     "",
        "isic_likely_section":      "",
        "likely_2digit":            "",
        "likely_3digit":            "",
        "likely_4digit":            "",
        "isic_format_unrecognized": ""
    }

    if stats["n_total"] == 0:
        return result

    result["present"] = "X"

    n_total    = stats["n_total"]
    n_section  = stats["n_section"]
    n_alphanum = stats["n_alphanum"]
    n_numeric  = stats["n_numeric"]

    pct_section  = 100 * n_section  / n_total
    pct_alphanum = 100 * n_alphanum / n_total

    if n_section > 0 or n_alphanum > 0:
        result["isic_section_present"] = "X"

    if pct_section >= THRESHOLD_SECTION:
        result["isic_likely_section"] = "Yes"
        result["likely_4digit"] = "No"
        result["likely_3digit"] = "No"
        result["likely_2digit"] = "No"

    elif pct_alphanum >= THRESHOLD_SECTION:
        result["isic_likely_section"] = "Yes"
        result["likely_4digit"] = "No"
        result["likely_3digit"] = "No"
        result["likely_2digit"] = "No"

    elif n_numeric > 0:
        result["isic_likely_section"] = "No"
        pct_last0  = 100 * stats["n_last0"]  / n_numeric
        pct_last00 = 100 * stats["n_last00"] / n_numeric
        result["likely_4digit"] = "Yes" if pct_last0  <  THRESHOLD_4DIGIT else "No"
        result["likely_3digit"] = "Yes" if (pct_last0 >= THRESHOLD_3DIGIT and pct_last00 < THRESHOLD_2DIGIT) else "No"
        result["likely_2digit"] = "Yes" if pct_last00 >= THRESHOLD_2DIGIT else "No"

        all_no = all(result[k] == "No" for k in ["likely_2digit", "likely_3digit", "likely_4digit"])
        if all_no:
            result["isic_format_unrecognized"] = "X"

    else:
        result["isic_format_unrecognized"] = "X"

    return result


# ==============================================================================
# SPARK HELPERS
# ==============================================================================

def get_modal_value(df, col_names, varname):
    """
    Returns the modal non-null, non-empty value of a column as a string,
    or empty string if the column is missing or fully empty.
    """
    if varname not in col_names:
        return ""
    row = (
        df.filter(F.col(varname).isNotNull())
          .filter(F.trim(F.col(varname).cast("string")) != "")
          .groupBy(varname)
          .count()
          .orderBy(F.desc("count"))
          .select(varname)
          .limit(1)
          .collect()
    )
    return str(row[0][0]).strip() if row else ""


def get_presence_flags(df, col_names, checkvars):
    """
    Checks all variables in a SINGLE aggregation query per table.
    Returns "X" if a variable exists and has at least one non-missing value,
    "" otherwise.
    """
    existing = [v for v in checkvars if v in col_names]
    if not existing:
        return {v: "" for v in checkvars}

    agg_exprs = [
        F.sum(F.when(F.col(v).isNotNull(), 1).otherwise(0)).alias(v)
        for v in existing
    ]
    counts = df.agg(*agg_exprs).collect()[0]

    return {
        v: ("X" if (v in existing and counts[v] and counts[v] > 0) else "")
        for v in checkvars
    }


def compute_isco_depth(df, col_names, isco_ver, isco_refs):
    """
    ISCO depth classification — method depends on ISCO version.

    ISCO-08 and ISCO-88: reference matching + trailing-zero signal
    (see classify_isco_reference).
    All other ISCO versions (including ISCO-68 and ISCO-58): trailing-zero
    logic applied directly (see classify_isco_trailing_zero).

    Armed Forces codes (starting with "0") are excluded from the
    trailing-zero calculation since they are legitimate 4-digit codes.

    Only one depth level is assigned per survey.
    Applied to both main job (occup_isco) and second job (occup_isco_2).

    isco_refs: output of build_isco_refs, loaded once per run.
    """
    ref_version = normalize_isco_version(isco_ver)
    ref = isco_refs.get(ref_version) if ref_version else None

    def _depth_for_col(colname):
        result = {
            "present":       "",
            "likely_4digit": "",
            "likely_3digit": "",
            "likely_2digit": ""
        }
        if colname not in col_names:
            return result

        base = (
            df.filter(F.col(colname).isNotNull())
              .filter(F.trim(F.col(colname).cast("string")) != "")
              .withColumn("code_str", F.trim(F.col(colname).cast("string")))
        )

        has_data = base.limit(1).count() > 0
        if not has_data:
            return result

        result["present"] = "X"

        if ref is not None:
            stats = (
                base
                .withColumn("match_class", ref["udf"](F.col("code_str")))
                # Exclude Armed Forces codes (start with "0") from trailing-zero
                # calculation — they are legitimate 4-digit codes ending in 0
                .withColumn("is_armed_forces", F.col("code_str").startswith("0").cast("int"))
                .withColumn("lastdigit",   F.expr("right(code_str, 1)"))
                .withColumn("last2digits", F.expr("right(code_str, 2)"))
                .agg(
                    F.count("*").alias("n_total"),
                    F.sum(F.when(F.col("match_class") == "4digit", 1).otherwise(0)).alias("n_4digit"),
                    F.sum(F.when(F.col("match_class") == "3digit", 1).otherwise(0)).alias("n_3digit"),
                    F.sum(F.when(F.col("match_class") == "2digit", 1).otherwise(0)).alias("n_2digit"),
                    F.sum(F.when(F.col("match_class") == "unknown", 1).otherwise(0)).alias("n_unknown"),
                    # Trailing zero counts exclude Armed Forces codes
                    F.sum(F.when(
                        (F.col("is_armed_forces") == 0) & (F.col("lastdigit") == "0"), 1
                    ).otherwise(0)).alias("n_last0"),
                    F.sum(F.when(
                        (F.col("is_armed_forces") == 0) & (F.col("last2digits") == "00"), 1
                    ).otherwise(0)).alias("n_last00"),
                    F.sum(F.when(F.col("is_armed_forces") == 0, 1).otherwise(0)).alias("n_non_af"),
                )
                .collect()[0]
            )

            depth, pct_unknown = classify_isco_reference(stats)

            # Warn if high unknown rate — classification may be unreliable
            if pct_unknown > 30:
                print(f"    NOTE: {colname} — {pct_unknown:.0f}% of codes unrecognised in {ref_version} reference. Classification may be unreliable.")

        else:
            stats = (
                base
                .withColumn("is_armed_forces", F.col("code_str").startswith("0").cast("int"))
                .withColumn("lastdigit",   F.expr("right(code_str, 1)"))
                .withColumn("last2digits", F.expr("right(code_str, 2)"))
                .agg(
                    F.sum(F.when(F.col("is_armed_forces") == 0, 1).otherwise(0)).alias("n_non_af"),
                    F.sum(F.when(
                        (F.col("is_armed_forces") == 0) & (F.col("lastdigit") == "0"), 1
                    ).otherwise(0)).alias("n_last0"),
                    F.sum(F.when(
                        (F.col("is_armed_forces") == 0) & (F.col("last2digits") == "00"), 1
                    ).otherwise(0)).alias("n_last00"),
                )
                .collect()[0]
            )

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
    """
    ISIC depth for main job (industrycat_isic) only. See classify_isic.
    """

    def _depth_for_col(colname):
        if colname not in col_names:
            return classify_isic({"n_total": 0})

        stats = (
            df.filter(F.col(colname).isNotNull())
              .filter(F.trim(F.col(colname).cast("string")) != "")
              .withColumn("code_trim",   F.trim(F.upper(F.col(colname).cast("string"))))
              .withColumn("is_section",  F.col("code_trim").rlike("^[A-U]$").cast("int"))
              .withColumn("is_alphanum", F.col("code_trim").rlike("^[A-U][0-9]+$").cast("int"))
              .withColumn("is_numeric",  F.col("code_trim").rlike("^[0-9]+$").cast("int"))
              .withColumn("lastdigit",   F.expr("right(code_trim, 1)"))
              .withColumn("last2digits", F.expr("right(code_trim, 2)"))
              .withColumn("last0",  F.when(
                  (F.col("is_numeric") == 1) & (F.col("lastdigit")   == "0"), 1).otherwise(0))
              .withColumn("last00", F.when(
                  (F.col("is_numeric") == 1) & (F.col("last2digits") == "00"), 1).otherwise(0))
              .agg(
                  F.count("*").alias("n_total"),
                  F.sum("is_section").alias("n_section"),
                  F.sum("is_alphanum").alias("n_alphanum"),
                  F.sum("is_numeric").alias("n_numeric"),
                  F.sum("last0").alias("n_last0"),
                  F.sum("last00").alias("n_last00"),
              )
              .collect()[0]
        )

        return classify_isic(stats)

    main = _depth_for_col("industrycat_isic")

    return {
        "isic_present":               main["present"],
        "isic_section_present":       main["isic_section_present"],
        "isic_likely_section":        main["isic_likely_section"],
        "isic_likely_2digit":         main["likely_2digit"],
        "isic_likely_3digit":         main["likely_3digit"],
        "isic_likely_4digit":         main["likely_4digit"],
        "isic_format_unrecognized":   main["isic_format_unrecognized"],
    }
