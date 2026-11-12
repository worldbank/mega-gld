# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# ==============================================================================
# platform_creation.py
#
# PURPOSE:
#   Queries harmonized GLD tables to check variable presence and ISCO/ISIC
#   coding depth for each country-survey-year.
#
# OUTPUT:
#   prd_mega.sgld48.gld_variable_coverage  (Delta table)
#
# NOTE:
#   This script must be run in Databricks.
#   Reference Excel file must be available at the path defined in CONFIG.
#   Helper functions live in helpers/platform_pipeline.py.
# ==============================================================================

import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from helpers.platform_pipeline import *

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
    # STEP 1B: Load ISCO reference code sets (once per run)
    #
    # ISCO-08 and ISCO-88 have 4-digit unit group codes in the ILO file.
    # ISCO-68 and ISCO-58 only have 2-digit/1-digit codes — these versions
    # fall back to trailing-zero logic in compute_isco_depth.
    # ==========================================================================

    print("Loading ISCO reference codes from ILO Excel file...")

    isco_refs = {
        "ISCO-08": load_isco_reference(EXCEL_ISCO, "ISCO_08"),
        "ISCO-88": load_isco_reference(EXCEL_ISCO, "ISCO_88"),
    }
    for version, (ref_4d, ref_3d, ref_2d) in isco_refs.items():
        print(f"{version} reference: {len(ref_4d)} 4-digit, {len(ref_3d)} 3-digit, {len(ref_2d)} 2-digit codes.")
    print("ISCO-68 and ISCO-58: no 4-digit reference available — trailing-zero logic will be applied.")

    # ==========================================================================
    # STEP 2: Loop over all surveys and compute coverage
    # ==========================================================================

    print("Computing variable coverage...")
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []

    for row in meta_df:
        key      = survey_key(row)
        country  = key["country"]
        year     = key["survey_year"]
        tbl_name = f"prd_csc_mega.sgld48.{row['table_name']}"

        print(f"  Processing: {country} {year} {key['survey']} {key['quarter']}".strip())

        try:
            survey_df = spark.table(tbl_name)
            col_names = survey_df.columns

            presence = add_composite_flags(get_presence_flags(survey_df, col_names, VARS_TO_CHECK))

            # In case a non-string isco_version needs to be checked, use the isco_string_check notebook
            isco_ver = get_modal_value(survey_df, col_names, "isco_version")

            isco_depth = compute_isco_depth(survey_df, col_names, isco_ver, isco_refs)
            isic_depth = compute_isic_depth(survey_df, col_names)

            if isic_depth["isic_format_unrecognized"] == "X":
                sample = (
                    survey_df.filter(F.col("industrycat_isic").isNotNull())
                    .select("industrycat_isic")
                    .limit(10)
                    .toPandas()["industrycat_isic"]
                    .tolist()
                )
                print(f"    WARNING: {country} {year} — ISIC present but format unrecognized. Sample: {sample}")

            flat_row = {
                **key,
                "harmonization": row["harmonization"] or "",
                "isco_version":  isco_ver,
                "isic_version":  get_modal_value(survey_df, col_names, "isic_version"),
                "icls_v":        get_modal_value(survey_df, col_names, "icls_v"),
                "survname":      get_modal_value(survey_df, col_names, "survname"),
                **isco_depth,
                **isic_depth,
                "generated_at":  generated_at,
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

    key_match = " AND ".join(f"target.{k} <=> source.{k}" for k in KEY_COLUMNS)

    # Fail loudly rather than clearing the table: an empty selection or a run where
    # every survey fails points to an upstream problem, not to an empty catalog.
    if not meta_df:
        raise RuntimeError("No surveys with stacking = TRUE found in _ingestion_metadata — output table left unchanged.")
    if not rows:
        raise RuntimeError("No surveys were processed successfully — output table left unchanged.")

    coverage_df = spark.createDataFrame(rows)

    if not spark.catalog.tableExists(OUTPUT_TABLE):
        print(f"{OUTPUT_TABLE} does not exist — creating it.")
        coverage_df.write.format("delta").saveAsTable(OUTPUT_TABLE)
    else:
        if TRUNCATE_BEFORE_RUN:
            print("TRUNCATE_BEFORE_RUN is True — clearing existing table...")
            spark.sql(f"TRUNCATE TABLE {OUTPUT_TABLE}")

        coverage_df.createOrReplaceTempView("coverage_updates")
        spark.sql(f"""
            MERGE INTO {OUTPUT_TABLE} AS target
            USING coverage_updates AS source
            ON {key_match}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    # Remove surveys that are no longer selected (e.g. the stacking winner for a
    # country-year changed). Compared against the full selected key set, not
    # coverage_updates, so surveys that failed this run are kept, not deleted.
    selected_keys = spark.createDataFrame(
        [survey_key(r) for r in meta_df],
        schema=", ".join(f"{k} string" for k in KEY_COLUMNS),
    ).distinct()
    selected_keys.createOrReplaceTempView("selected_keys")
    spark.sql(f"""
        MERGE INTO {OUTPUT_TABLE} AS target
        USING selected_keys AS source
        ON {key_match}
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE
    """)

    after_count = spark.table(OUTPUT_TABLE).count()
    print(f"Write complete. {len(rows)} surveys processed. Table now has {after_count} rows.")
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
