# Databricks notebook source


# COMMAND ----------

#==============================================================================
# gld_variable_coverage_writer.py
#
# PURPOSE:
#   Queries harmonized GLD tables to check variable presence
#   for each country-survey-year.
#   
#   
# OUTPUT:
#   prd_mega.sgld48.gld_variable_coverage  (Delta table)

# ==============================================================================

from datetime import datetime, timezone
from pyspark.sql import functions as F

# ==============================================================================
# CONFIG
# ==============================================================================

META_TABLE   = "prd_csc_mega.sgld48._ingestion_metadata"
OUTPUT_TABLE = "prd_mega.sgld48.gld_variable_coverage"

# ==============================================================================
# VARIABLE GROUPS
# 
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

# Flat list of all variables to check
ALL_CHECKVARS = [v for group in VARIABLE_GROUPS.values() for v in group]

# Informality proxies — used to compute the derived "informality" flag
INFORMALITY_PROXIES = ["contract", "healthins", "socialsec", "whours", "unitwage"]

# ==============================================================================
# STEP 1: Read _ingestion_metadata to get the stacking universe
# ==============================================================================

print("Reading _ingestion_metadata for stacked surveys...")

meta_df = (
    spark.table(META_TABLE)
    .filter(F.col("stacking") == True)
    .filter(F.col("table_name").isNotNull())
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


def compute_isco_depth(df, col_names):
    """
    ISCO code depth analysis.
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
    ISIC code depth analysis.
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
rows = []

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
        all_vars_to_check = ALL_CHECKVARS + [
            v for v in INFORMALITY_PROXIES if v not in ALL_CHECKVARS
        ] + ["wage_no_compen"]
        presence = get_presence_flags(survey_df, col_names, all_vars_to_check)

        # Derived flags
        presence["informality"]       = "X" if any(presence.get(v) == "X" for v in INFORMALITY_PROXIES) else ""
        presence["real_monthly_wage"] = "X" if presence.get("wage_no_compen") == "X" else ""

        # Code depth (one query each)
        isco_depth = compute_isco_depth(survey_df, col_names)
        isic_depth = compute_isic_depth(survey_df, col_names)

        # Version/metadata fields
        isco_ver = get_first_value(survey_df, col_names, "isco_version")
        isic_ver = get_first_value(survey_df, col_names, "isic_version")
        icls_v   = get_first_value(survey_df, col_names, "icls_v")
        survname = get_first_value(survey_df, col_names, "survname")

        # Build flat row — all fields at the top level (no nested dicts)
        flat_row = {
            "country":          country,
            "survey_year":      year,
            "survey":           survey,
            "harmonization":    row["harmonization"] or "",
            "isco_version":     isco_ver,
            "isic_version":     isic_ver,
            "icls_v":           icls_v,
            "survname":         survname,
            # ISCO depth columns (prefixed to avoid collision)
            "isco_present":       isco_depth["present"],
            "isco_likely_4digit": isco_depth["likely_4digit"],
            "isco_likely_3digit": isco_depth["likely_3digit"],
            "isco_likely_2digit": isco_depth["likely_2digit"],
            # ISIC depth columns (prefixed to avoid collision)
            "isic_present":         isic_depth["present"],
            "isic_section_present": isic_depth["section_present"],
            "isic_likely_2digit":   isic_depth["likely_2digit"],
            "isic_likely_3digit":   isic_depth["likely_3digit"],
            "isic_likely_4digit":   isic_depth["likely_4digit"],
            # Timestamp
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # Flatten all variable presence flags into the row
        flat_row.update(presence)
        rows.append(flat_row)

    except Exception as e:
        print(f"    WARNING: Could not process {tbl_name} — {e}")
        continue

print(f"Successfully processed {len(rows)} surveys.")

# ==============================================================================
# STEP 4: Create table , then MERGE
# ==============================================================================

coverage_df = spark.createDataFrame(rows)

# Register as a temp view so we can reference it in the MERGE SQL
coverage_df.createOrReplaceTempView("coverage_updates")

# Create the target table on first run if it doesn't exist yet.
# Schema is inferred from the first batch of data.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE}
    USING DELTA
    AS SELECT * FROM coverage_updates WHERE 1=0
""")

# MERGE:
#   - Update existing rows if any field changed
#   - Insert new surveys not yet in the table
#   - Delete surveys no longer in the stacking universe
spark.sql(f"""
    MERGE INTO {OUTPUT_TABLE} AS target
    USING coverage_updates AS source
    ON  target.country     <=> source.country
    AND target.survey_year <=> source.survey_year
    AND target.survey      <=> source.survey
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE
""")

# Log row counts for audit trail
after_count = spark.table(OUTPUT_TABLE).count()
print(f"MERGE complete. {len(rows)} surveys processed. Table now has {after_count} rows.")
print(f"Output: {OUTPUT_TABLE}")

# ==============================================================================
# STEP 5: Quick preview
# ==============================================================================

import pandas as pd

preview_cols = ["country", "survey_year", "survey", "harmonization",
                "lstatus", "empstat", "wage_no_compen", "isco_present", "isic_present",
                "generated_at"]

display(
    spark.table(OUTPUT_TABLE)
    .select(*preview_cols)
    .orderBy("country", "survey_year")
    .limit(50)
    .toPandas()
)


# COMMAND ----------

spark.table(META_TABLE).select("stacking").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


spark.sql("SHOW TABLES IN prd_mega.gld LIKE 'gld_variable_coverage'").show()

# COMMAND ----------

import sqlite3
import pandas as pd
import os

# Step 1: Read the Delta table into a pandas DataFrame
print("Reading Delta table...")
df = spark.table("prd_mega.gld.gld_variable_coverage").toPandas()
print(f"Loaded {len(df)} rows and {len(df.columns)} columns.")

# Step 2: Save to SQLite
db_path = "/tmp/gld_variable_coverage.db"
conn = sqlite3.connect(db_path)
df.to_sql("gld_variable_coverage", conn, if_exists="replace", index=False)
conn.close()
print(f"SQLite database created at {db_path}")
dbutils.fs.cp(f"file:{db_path}", "dbfs:/FileStore/gld_variable_coverage.db")
# Save as CSV instead
df.to_csv("/tmp/gld_variable_coverage.csv", index=False)

# Display download link
displayHTML('<a href="files/gld_variable_coverage.csv">Click here to download the CSV file</a>')


# COMMAND ----------

# Display the file for download
with open("/tmp/gld_variable_coverage.db", "rb") as f:
    data = f.read()

import base64
b64 = base64.b64encode(data).decode()
displayHTML(f'''
    <a href="data:application/octet-stream;base64,{b64}" 
       download="gld_variable_coverage.db">
       Click here to download gld_variable_coverage.db
    </a>
''')

