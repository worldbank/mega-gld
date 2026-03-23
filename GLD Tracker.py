# Databricks notebook source
# ==============================================================================
# variable_coverage_table.py
#
# PURPOSE:
#   Queries harmonized GLD tables to check variable presence
#   (at least one non-missing value) for each country-survey-year.
#   Produces a JSON file for the GLD website Survey Finder.
#
# OUTPUT:
#   variable_coverage.json  
#

# ==============================================================================

import json
from datetime import datetime, timezone
from pyspark.sql import functions as F

# ==============================================================================
# CONFIG
# ==============================================================================

META_TABLE       = "prd_csc_mega.sgld48._ingestion_metadata"

JSON_OUTPUT_PATH = "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/gld_website/variable_coverage.json"
# ==============================================================================
# VARIABLE GROUPS
# Edit here to add/remove variables or rename categories.
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

# Flat list of all variables to check directly (derived flags handled separately)
ALL_CHECKVARS = [v for group in VARIABLE_GROUPS.values() for v in group]

# Informality proxies — used to compute the derived "informality" flag
INFORMALITY_PROXIES = ["contract", "healthins", "socialsec", "whours", "unitwage"]

# ==============================================================================
# STEP 1: Read _ingestion_metadata to get the stacking universe
# ==============================================================================

print("Reading _ingestion_metadata for stacked surveys...")

meta_df = (
    spark.table(META_TABLE)
    .filter("stacking = 1")
    .filter("table_name IS NOT NULL")
    .select("country", "year", "survey", "table_name", "harmonization")
    .collect()
)

print(f"Found {len(meta_df)} country-survey-year rows to process.")

# ==============================================================================
# STEP 2: Helper functions
# ==============================================================================

def get_first_value(df, col_names, varname):
    """Returns the first non-null value of a column as a string, or empty string."""
    if varname not in col_names:
        return ""
    row = df.filter(F.col(varname).isNotNull()).select(varname).limit(1).collect()
    return str(row[0][0]) if row else ""


def get_presence_flags(df, col_names, checkvars):
    """
    Checks all variables in a SINGLE aggregation query per table.
    Returns "X" if a variable exists and has at least one non-missing value,
    "" otherwise. This correctly handles columns that exist but are entirely empty.
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


def compute_isco_depth(df, col_names):
    """
    ISCO code depth analysis — mirrors the % threshold method from
    tracker_withisco_isic.do (the latter method in the Stata code).
    Runs as a single aggregation query.
    Thresholds: likely_4digit if <75% end in 0
                likely_3digit if >=80% end in 0
                likely_2digit if >=80% end in 00
    """
    result = {
        "present":       "",
        "likely_4digit": "",
        "likely_3digit": "",
        "likely_2digit": ""
    }
    if "occup_isco" not in col_names:
        return result

    stats = (
        df.filter(F.col("occup_isco").isNotNull())
        .withColumn("code_str",    F.col("occup_isco").cast("string"))
        .withColumn("lastdigit",   F.expr("right(code_str, 1)"))
        .withColumn("last2digits", F.expr("right(code_str, 2)"))
        .agg(
            F.count("*").alias("n_total"),
            F.sum(F.when(F.col("lastdigit")   == "0",  1).otherwise(0)).alias("n_last0"),
            F.sum(F.when(F.col("last2digits") == "00", 1).otherwise(0)).alias("n_last00")
        )
        .collect()[0]
    )

    if stats["n_total"] > 0:
        result["present"]       = "X"
        pct_last0  = 100 * stats["n_last0"]  / stats["n_total"]
        pct_last00 = 100 * stats["n_last00"] / stats["n_total"]
        result["likely_4digit"] = "Yes" if pct_last0  <  75 else "No"
        result["likely_3digit"] = "Yes" if pct_last0  >= 80 else "No"
        result["likely_2digit"] = "Yes" if pct_last00 >= 80 else "No"

    return result


def compute_isic_depth(df, col_names):
    """
    ISIC code depth analysis — mirrors the regex/pattern method from
    tracker_withisco_isic.do (the latter method in the Stata code).
    Runs as a single aggregation query.
    Classifies codes as: section (A-U), 2-digit, 3-digit, or 4-digit
    based on majority share (>=50% threshold).
    """
    result = {
        "present":         "",
        "section_present": "",
        "likely_2digit":   "",
        "likely_3digit":   "",
        "likely_4digit":   ""
    }
    if "industrycat_isic" not in col_names:
        return result

    stats = (
        df.filter(F.col("industrycat_isic").isNotNull())
        .withColumn("code_trim", F.trim(F.upper(F.col("industrycat_isic").cast("string"))))
        .agg(
            F.count("*").alias("n_total"),
            F.sum(F.when(F.col("code_trim").rlike("^[A-U]$"),    1).otherwise(0)).alias("n_sec"),
            F.sum(F.when(F.col("code_trim").rlike("^[0-9]{2}$"), 1).otherwise(0)).alias("n_div2"),
            F.sum(F.when(F.col("code_trim").rlike("^[0-9]{3}$"), 1).otherwise(0)).alias("n_grp3"),
            F.sum(F.when(F.col("code_trim").rlike("^[0-9]{4}$"), 1).otherwise(0)).alias("n_cls4")
        )
        .collect()[0]
    )

    if stats["n_total"] > 0:
        pct_div2 = 100 * stats["n_div2"] / stats["n_total"]
        pct_grp3 = 100 * stats["n_grp3"] / stats["n_total"]
        pct_cls4 = 100 * stats["n_cls4"] / stats["n_total"]
        result["present"]         = "X"
        result["section_present"] = "X" if stats["n_sec"] > 0 else ""
        result["likely_2digit"]   = "Yes" if pct_div2 >= 50 else "No"
        result["likely_3digit"]   = "Yes" if pct_grp3 >= 50 else "No"
        result["likely_4digit"]   = "Yes" if pct_cls4 >= 50 else "No"

    return result


# ==============================================================================
# STEP 3: Loop over all surveys and compute coverage
# ==============================================================================

print("Computing variable coverage...")
coverage_list = []

for row in meta_df:
    country  = row["country"]
    year     = str(row["year"])
    survey   = row["survey"]
    tbl_name = f"prd_csc_mega.sgld48.{row['table_name']}"

    print(f"  Processing: {country} {year} {survey}")

    try:
        survey_df = spark.table(tbl_name)
        col_names = survey_df.columns

        # All variable presence checks in ONE query per table
        all_vars_to_check = ALL_CHECKVARS + [v for v in INFORMALITY_PROXIES if v not in ALL_CHECKVARS] + ["wage_no_compen"]
        presence = get_presence_flags(survey_df, col_names, all_vars_to_check)

        # Derived flags
        presence["informality"]       = "X" if any(presence.get(v) == "X" for v in INFORMALITY_PROXIES) else ""
        presence["real_monthly_wage"] = "X" if presence.get("wage_no_compen") == "X" else ""

        # Code depth analysis (one query each)
        isco_depth = compute_isco_depth(survey_df, col_names)
        isic_depth = compute_isic_depth(survey_df, col_names)

        # Version fields — read from survey table if columns exist
        isco_ver = get_first_value(survey_df, col_names, "isco_version")
        isic_ver = get_first_value(survey_df, col_names, "isic_version")
        icls_v   = get_first_value(survey_df, col_names, "icls_v")
        survname = get_first_value(survey_df, col_names, "survname")

        coverage_list.append({
            "country":                   country,
            "survey_year":               year,
            "survey":                    survey,
            "harmonization":             row["harmonization"] or "",
            "isco_version":              isco_ver,
            "isic_version":              isic_ver,
            "icls_v":                    icls_v,
            "survname":                  survname,
            "variables":                 presence,
            "isco_depth":                isco_depth,
            "isic_depth":                isic_depth
        })

    except Exception as e:
        print(f"    WARNING: Could not process {tbl_name} — {e}")
        continue

print(f"Successfully processed {len(coverage_list)} surveys.")

# ==============================================================================
# STEP 4: Build JSON output
# ==============================================================================

output = {
    "metadata": {
        "generated_at":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_surveys":       len(coverage_list),
        "variable_groups": VARIABLE_GROUPS
    },
    "surveys": coverage_list
}

# ==============================================================================
# STEP 5: Write JSON 
# ==============================================================================

import os

json_str = json.dumps(output, indent=2, ensure_ascii=False)

# Write using plain Python file I/O instead of dbutils
output_path = JSON_OUTPUT_PATH

with open(output_path, "w", encoding="utf-8") as f:
    f.write(json_str)

print("Done! variable_coverage.json written successfully.")
print(f"Output: {output_path}")

import pandas as pd

# Build a flat table from the coverage list
rows = []
for s in coverage_list:
    row = {
        "Country":     s["country"],
        "Survey":      s["survey"],
        "Year":        s["survey_year"],
    }
    # Add all variable presence flags
    row.update(s["variables"])
    rows.append(row)

df_preview = pd.DataFrame(rows)

# Show it
display(df_preview)

# COMMAND ----------

import pandas as pd

# Build a flat table from the coverage list
rows = []
for s in coverage_list:
    row = {
        "Country":     s["country"],
        "Survey":      s["survey"],
        "Year":        s["survey_year"],
    }
    # Add all variable presence flags
    row.update(s["variables"])
    rows.append(row)

df_preview = pd.DataFrame(rows)

# Show it
display(df_preview)

# COMMAND ----------

from pyspark.sql import functions as F

# Check BGD 2005 directly
tables = spark.sql("SHOW TABLES IN sgld48 LIKE 'bgd*'").collect()
for t in tables:
    print(t)

# COMMAND ----------

tables = spark.sql("SHOW TABLES IN prd_csc_mega.sgld48 LIKE 'bgd*'").collect()
for t in tables:
    print(t)
