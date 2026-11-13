# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# ==============================================================================
# platform_creation.py
#
# PURPOSE:
#   Queries harmonized GLD tables to check variable presence
#   for each country-survey-year.
#
# OUTPUT:
#   prd_mega.sgld48.gld_variable_coverage  (Delta table)
#
# NOTE:
#   This script must be run in Databricks.
#   Reference Excel file must be available at the path defined in CONFIG.
#   Thresholds, variable groups and helper functions are in
#   helpers/platform_pipeline.py.
# ==============================================================================

import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run "./helpers/platform_pipeline"

# COMMAND ----------

# ==============================================================================
# CONFIG
# ==============================================================================

META_TABLE   = "prd_csc_mega.sgld48._ingestion_metadata"
OUTPUT_TABLE = "prd_mega.sgld48.gld_variable_coverage"

# ILO combined ISCO reference file — contains ISCO-08, ISCO-88, ISCO-68, ISCO-58
# Each version is on a separate sheet with a "unit" column of unit group codes
EXCEL_ISCO   = "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/Classification_ISCO/ISCO.xlsx"

# Set to True only when rebuilding the table from scratch
TRUNCATE_BEFORE_RUN = False


# ==============================================================================
# DATABRICKS GUARD
# ==============================================================================

def in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

if not in_databricks():
    print("Not running in Databricks. Skipping platform creation.")

else:
    spark = SparkSession.builder.getOrCreate()

    # ==========================================================================
    # STEP 1: Read _ingestion_metadata
    # ==========================================================================

    print("Reading _ingestion_metadata for stacked surveys...")

    meta_df = (
        spark.table(META_TABLE)
        .filter(F.col("stacking") == True)
        .filter(F.col("table_name").isNotNull())
        .select("country", "year", "survey", "quarter", "table_name", "harmonization")
        .collect()
    )

    print(f"Found {len(meta_df)} country-survey-year rows to process.")

    # ==========================================================================
    # STEP 1B: Load ISCO reference code sets
    #
    # Source: ILO combined ISCO Excel file (ISCO.xlsx)
    # Each version is on a separate sheet. The "unit" column contains the
    # genuine unit group codes (4-digit for ISCO-08 and ISCO-88).
    #
    # ISCO-68 and ISCO-58 only have 2-digit and 1-digit codes respectively —
    # these versions fall back to trailing-zero logic in compute_isco_depth.
    #
    # Reference sets and their classification UDFs are built once here and
    # reused for every survey.
    # ==========================================================================

    print("Loading ISCO reference codes from ILO Excel file...")

    isco_refs = build_isco_refs(EXCEL_ISCO)
    for version, ref in isco_refs.items():
        print(f"{version} reference: {len(ref['4digit'])} 4-digit, {len(ref['3digit'])} 3-digit, {len(ref['2digit'])} 2-digit codes.")

    print("ISCO-68 and ISCO-58: no 4-digit reference available — trailing-zero logic will be applied.")

    # ==========================================================================
    # STEP 2: Loop over all surveys and compute coverage
    # ==========================================================================

    print("Computing variable coverage...")
    rows = []

    for row in meta_df:
        country  = row["country"]
        year     = str(row["year"])
        survey   = row["survey"]
        quarter  = str(row["quarter"]).strip() if row["quarter"] else ""
        tbl_name = f"prd_csc_mega.sgld48.{row['table_name']}"

        print(f"  Processing: {country} {year} {survey} {quarter}".strip())

        try:
            survey_df = spark.table(tbl_name)
            col_names = survey_df.columns

            presence = get_presence_flags(survey_df, col_names, ALL_VARS_TO_CHECK)
            # Derive composite flags from individual variable presence
            presence["informality"]    = "X" if any(presence.get(v) == "X" for v in INFORMALITY_PROXIES) else ""
            presence["wage_available"] = "X" if presence.get("wage_no_compen") == "X" else ""

            # Get isco_version. In case non-string isco_version needs to be checked - use notebook named isco_string_check
            isco_ver = get_modal_value(survey_df, col_names, "isco_version")

            isco_depth = compute_isco_depth(survey_df, col_names, isco_ver, isco_refs)
            isic_depth = compute_isic_depth(survey_df, col_names)

            if isic_depth.get("isic_format_unrecognized") == "X":
                sample = (
                    survey_df.filter(F.col("industrycat_isic").isNotNull())
                    .select("industrycat_isic")
                    .limit(10)
                    .toPandas()["industrycat_isic"]
                    .tolist()
                )
                print(f"    WARNING: {country} {year} — ISIC present but format unrecognized. Sample: {sample}")

            isic_ver = get_modal_value(survey_df, col_names, "isic_version")
            icls_v   = get_modal_value(survey_df, col_names, "icls_v")
            survname = get_modal_value(survey_df, col_names, "survname")

            flat_row = {
                "country":               country,
                "survey_year":           year,
                "survey":                survey,
                "quarter":               quarter,
                "harmonization":         row["harmonization"] or "",
                "isco_version":          isco_ver,
                "isic_version":          isic_ver,
                "icls_v":                icls_v,
                "survname":              survname,
                "isco_present":          isco_depth["isco_present"],
                "isco_likely_4digit":    isco_depth["isco_likely_4digit"],
                "isco_likely_3digit":    isco_depth["isco_likely_3digit"],
                "isco_likely_2digit":    isco_depth["isco_likely_2digit"],
                "isco_2_present":        isco_depth["isco_2_present"],
                "isco_2_likely_4digit":  isco_depth["isco_2_likely_4digit"],
                "isco_2_likely_3digit":  isco_depth["isco_2_likely_3digit"],
                "isco_2_likely_2digit":  isco_depth["isco_2_likely_2digit"],
                "isic_present":          isic_depth["isic_present"],
                "isic_section_present":  isic_depth["isic_section_present"],
                "isic_likely_section":   isic_depth["isic_likely_section"],
                "isic_likely_2digit":    isic_depth["isic_likely_2digit"],
                "isic_likely_3digit":    isic_depth["isic_likely_3digit"],
                "isic_likely_4digit":    isic_depth["isic_likely_4digit"],
                "isic_format_unrecognized": isic_depth["isic_format_unrecognized"],
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            flat_row.update(presence)
            rows.append(flat_row)

        except Exception as e:
            print(f"    WARNING: Could not process {tbl_name} — {e}")
            continue

    print(f"Successfully processed {len(rows)} surveys.")

    # ==========================================================================
    # STEP 3: Write to Delta table
    # ==========================================================================

    if TRUNCATE_BEFORE_RUN:
        print("TRUNCATE_BEFORE_RUN is True — clearing existing table...")
        spark.sql(f"TRUNCATE TABLE {OUTPUT_TABLE}")

    coverage_df = spark.createDataFrame(rows)
    coverage_df.createOrReplaceTempView("coverage_updates")

    spark.sql(f"""
        MERGE INTO {OUTPUT_TABLE} AS target
        USING coverage_updates AS source
        ON  target.country     <=> source.country
        AND target.survey_year <=> source.survey_year
        AND target.survey      <=> source.survey
        AND target.quarter     <=> source.quarter
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    after_count = spark.table(OUTPUT_TABLE).count()
    print(f"MERGE complete. {len(rows)} surveys processed. Table now has {after_count} rows.")
    print(f"Output: {OUTPUT_TABLE}")

    # ==========================================================================
    # STEP 4: Quick preview
    # ==========================================================================

    preview_cols = [
        "country", "survey_year", "survey", "quarter", "harmonization",
        "isco_version", "isco_present", "isco_likely_4digit",
        "isco_likely_3digit", "isco_likely_2digit",
        "isic_present", "isic_likely_4digit", "generated_at"
    ]

    display(
        spark.table(OUTPUT_TABLE)
        .select(*preview_cols)
        .orderBy("country", "survey_year")
        .limit(50)
        .toPandas()
    )
