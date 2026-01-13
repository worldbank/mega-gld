# Databricks notebook source
TABLE_QULIFIER =  "prd_csc_mega.sgld48."
metadata_table = f"{TABLE_QULIFIER}_ingestion_metadata"
metadata = spark.table(metadata_table)
OFFICIAL_CLASS = 'Official Use'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("countrycode", StringType(), True),
    StructField("survname", StringType(), True),
    StructField("survey", StringType(), True),
    StructField("icls_v", StringType(), True),
    StructField("isced_version", StringType(), True),
    StructField("isco_version", StringType(), True),
    StructField("isic_version", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("vermast", StringType(), True),
    StructField("veralt", StringType(), True),
    StructField("harmonization", StringType(), True),
    StructField("int_year", IntegerType(), True),
    StructField("int_month", IntegerType(), True),
    StructField("hhid", StringType(), True),
    StructField("pid", StringType(), True),
    StructField("weight", DoubleType(), True),
    StructField("weight_m", DoubleType(), True),
    StructField("weight_q", DoubleType(), True),
    StructField("psu", StringType(), True),
    StructField("ssu", StringType(), True),
    StructField("wave", StringType(), True),
    StructField("panel", StringType(), True),
    StructField("visit_no", IntegerType(), True),
    StructField("urban", IntegerType(), True),
    StructField("subnatidsurvey", StringType(), True),
    StructField("strata", StringType(), True),
    StructField("hsize", DoubleType(), True),
    StructField("age", DoubleType(), True),
    StructField("male", IntegerType(), True),
    StructField("relationharm", IntegerType(), True),
    StructField("relationcs", StringType(), True),
    StructField("marital", IntegerType(), True),
    StructField("eye_dsablty", IntegerType(), True),
    StructField("hear_dsablty", IntegerType(), True),
    StructField("walk_dsablty", IntegerType(), True),
    StructField("conc_dsord", IntegerType(), True),
    StructField("slfcre_dsablty", IntegerType(), True),
    StructField("comm_dsablty", IntegerType(), True),
    StructField("migrated_mod_age", IntegerType(), True),
    StructField("migrated_ref_time", IntegerType(), True),
    StructField("migrated_binary", IntegerType(), True),
    StructField("migrated_years", IntegerType(), True),
    StructField("migrated_from", IntegerType(), True),
    StructField("migrated_from_cat", IntegerType(), True),
    StructField("migrated_from_code", IntegerType(), True),
    StructField("migrated_from_country", StringType(), True),
    StructField("migrated_reason", IntegerType(), True),
    StructField("ed_mod_age", IntegerType(), True),
    StructField("school", IntegerType(), True),
    StructField("literacy", IntegerType(), True),
    StructField("educy", IntegerType(), True),
    StructField("educat7", IntegerType(), True),
    StructField("educat5", IntegerType(), True),
    StructField("educat4", IntegerType(), True),
    StructField("educat_orig", StringType(), True),
    StructField("educat_isced", DoubleType(), True),
    StructField("vocational", IntegerType(), True),
    StructField("vocational_type", IntegerType(), True),
    StructField("vocational_length_l", IntegerType(), True),
    StructField("vocational_length_u", IntegerType(), True),
    StructField("vocational_financed", IntegerType(), True),
    StructField("minlaborage", DoubleType(), True),
    StructField("lstatus", IntegerType(), True),
    StructField("potential_lf", IntegerType(), True),
    StructField("underemployment", IntegerType(), True),
    StructField("nlfreason", IntegerType(), True),
    StructField("unempldur_l", DoubleType(), True),
    StructField("unempldur_u", DoubleType(), True),
    StructField("empstat", IntegerType(), True),
    StructField("ocusec", IntegerType(), True),
    StructField("industry_orig", StringType(), True),
    StructField("industrycat_isic", StringType(), True),
    StructField("industrycat10", IntegerType(), True),
    StructField("industrycat4", IntegerType(), True),
    StructField("occup_orig", StringType(), True),
    StructField("occup_isco", StringType(), True),
    StructField("occup_skill", IntegerType(), True),
    StructField("occup", IntegerType(), True),
    StructField("wage_no_compen", DoubleType(), True),
    StructField("unitwage", IntegerType(), True),
    StructField("whours", DoubleType(), True),
    StructField("wmonths", DoubleType(), True),
    StructField("wage_total", DoubleType(), True),
    StructField("contract", IntegerType(), True),
    StructField("healthins", IntegerType(), True),
    StructField("socialsec", IntegerType(), True),
    StructField("union", IntegerType(), True),
    StructField("firmsize_l", DoubleType(), True),
    StructField("firmsize_u", DoubleType(), True),
    StructField("empstat_2", IntegerType(), True),
    StructField("ocusec_2", IntegerType(), True),
    StructField("industry_orig_2", StringType(), True),
    StructField("industrycat_isic_2", IntegerType(), True),
    StructField("industrycat10_2", IntegerType(), True),
    StructField("industrycat4_2", IntegerType(), True),
    StructField("occup_orig_2", StringType(), True),
    StructField("occup_isco_2", StringType(), True),
    StructField("occup_skill_2", IntegerType(), True),
    StructField("occup_2", IntegerType(), True),
    StructField("wage_no_compen_2", DoubleType(), True),
    StructField("unitwage_2", IntegerType(), True),
    StructField("whours_2", DoubleType(), True),
    StructField("wmonths_2", DoubleType(), True),
    StructField("wage_total_2", DoubleType(), True),
    StructField("firmsize_l_2", DoubleType(), True),
    StructField("firmsize_u_2", DoubleType(), True),
    StructField("t_hours_others", DoubleType(), True),
    StructField("t_wage_nocompen_others", DoubleType(), True),
    StructField("t_wage_others", DoubleType(), True),
    StructField("t_hours_total", DoubleType(), True),
    StructField("t_wage_nocompen_total", DoubleType(), True),
    StructField("t_wage_total", DoubleType(), True),
    StructField("lstatus_year", IntegerType(), True),
    StructField("potential_lf_year", IntegerType(), True),
    StructField("underemployment_year", IntegerType(), True),
    StructField("nlfreason_year", IntegerType(), True),
    StructField("unempldur_l_year", DoubleType(), True),
    StructField("unempldur_u_year", DoubleType(), True),
    StructField("empstat_year", IntegerType(), True),
    StructField("ocusec_year", IntegerType(), True),
    StructField("industry_orig_year", StringType(), True),
    StructField("industrycat_isic_year", IntegerType(), True),
    StructField("industrycat10_year", IntegerType(), True),
    StructField("industrycat4_year", IntegerType(), True),
    StructField("occup_orig_year", StringType(), True),
    StructField("occup_isco_year", StringType(), True),
    StructField("occup_skill_year", IntegerType(), True),
    StructField("occup_year", IntegerType(), True),
    StructField("wage_no_compen_year", DoubleType(), True),
    StructField("unitwage_year", IntegerType(), True),
    StructField("whours_year", DoubleType(), True),
    StructField("wmonths_year", DoubleType(), True),
    StructField("wage_total_year", DoubleType(), True),
    StructField("contract_year", IntegerType(), True),
    StructField("healthins_year", IntegerType(), True),
    StructField("socialsec_year", IntegerType(), True),
    StructField("union_year", IntegerType(), True),
    StructField("firmsize_l_year", DoubleType(), True),
    StructField("firmsize_u_year", DoubleType(), True),
    StructField("empstat_2_year", IntegerType(), True),
    StructField("ocusec_2_year", IntegerType(), True),
    StructField("industry_orig_2_year", StringType(), True),
    StructField("industrycat_isic_2_year", IntegerType(), True),
    StructField("industrycat10_2_year", IntegerType(), True),
    StructField("industrycat4_2_year", IntegerType(), True),
    StructField("occup_orig_2_year", StringType(), True),
    StructField("occup_isco_2_year", StringType(), True),
    StructField("occup_skill_2_year", IntegerType(), True),
    StructField("occup_2_year", IntegerType(), True),
    StructField("wage_no_compen_2_year", DoubleType(), True),
    StructField("unitwage_2_year", IntegerType(), True),
    StructField("whours_2_year", DoubleType(), True),
    StructField("wmonths_2_year", DoubleType(), True),
    StructField("wage_total_2_year", DoubleType(), True),
    StructField("firmsize_l_2_year", DoubleType(), True),
    StructField("firmsize_u_2_year", DoubleType(), True),
    StructField("t_hours_others_year", DoubleType(), True),
    StructField("t_wage_nocompen_others_year", DoubleType(), True),
    StructField("t_wage_others_year", DoubleType(), True),
    StructField("t_hours_total_year", DoubleType(), True),
    StructField("t_wage_nocompen_total_year", DoubleType(), True),
    StructField("t_wage_total_year", DoubleType(), True),
    StructField("njobs", DoubleType(), True),
    StructField("t_hours_annual", DoubleType(), True),
    StructField("linc_nc", DoubleType(), True),
    StructField("laborincome", DoubleType(), True)
])

# COMMAND ----------

#TODO add this to the metadata table as a flag
TO_REMOVE = ['MEX_2005-2023_ENOE_V01_M_V01_A_GLD', 'IND_2022_PLFS-Urban-Panel_V01_M_V01_A_GLD']

# COMMAND ----------

# Delete the harmonized Delta table if it already exists
HARMONIZED_CONFIDENTIAL = f"{TABLE_QULIFIER}GLD_HARMONIZED_CONFIDENTIAL"
HARMONIZED_OFFICIAL = f"{TABLE_QULIFIER}GLD_HARMONIZED_OFFICIAL"

if spark.catalog.tableExists(HARMONIZED_CONFIDENTIAL):
    spark.sql(f"DROP TABLE {HARMONIZED_CONFIDENTIAL}")
    print(f"Deleted existing table {HARMONIZED_CONFIDENTIAL}")

if spark.catalog.tableExists(HARMONIZED_OFFICIAL):
    spark.sql(f"DROP TABLE {HARMONIZED_OFFICIAL}")
    print(f"Deleted existing table {HARMONIZED_OFFICIAL}")

# COMMAND ----------

from pyspark.sql.functions import col, lit
import re

subnational_pattern = r"^subnatid\d+(_prev)?$"
gaul_pattern = r"^gaul_adm\d+_code$"

# Get distinct table names from the ingestion metadata
# Get distinct table names from the ingestion metadata where ingested is True
table_metadata = (
    metadata.filter(col("ingested") == True)
            .select("table_name", "classification", "country", "year", "survey")
            .distinct()
            .rdd.map(lambda r: (r["table_name"], r["classification"], r["country"], r["year"], r["survey"]))
            .collect()
)
# List of expected columns from the predefined schema
expected_cols = [f.name for f in schema.fields]

for tbl, classification, country_val, year_val, survey_val in table_metadata:
    src_df = spark.table(TABLE_QULIFIER + tbl)
    
    selected_exprs = []
    
    for c in expected_cols:
        if c == "countrycode" and country_val is not None:
            selected_exprs.append(lit(country_val).cast(schema[c].dataType).alias(c))
        elif c == "survname" and survey_val is not None:
            selected_exprs.append(lit(survey_val).cast(schema[c].dataType).alias(c))
        elif c in src_df.columns:
            # Column exists in source, cast it to master schema type
            selected_exprs.append(col(c).cast(schema[c].dataType).alias(c))
        else:
            # Column is MISSING in source, fill with NULL
            selected_exprs.append(lit(None).cast(schema[c].dataType).alias(c))

    # These aren't in the fixed schema but we want to keep them if they exist
    dynamic_cols = [c for c in src_df.columns if re.match(subnational_pattern, c) or re.match(gaul_pattern, c)]
    for dc in dynamic_cols:
        # Avoid adding if already added via expected_cols
        if dc not in expected_cols:
            selected_exprs.append(col(dc).cast("string").alias(dc))

    # Identify truly "Extra" columns for logging
    extra_cols = [c for c in src_df.columns if c not in expected_cols and c not in dynamic_cols]
    if extra_cols:
        print(f"Extra columns ignored: {extra_cols} for {country_val} {year_val}")

    # Create the aligned DataFrame
    aligned_df = src_df.select(*selected_exprs)

    # Union all tables together
    aligned_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(HARMONIZED_CONFIDENTIAL)
    if classification == OFFICIAL_CLASS:
        aligned_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(HARMONIZED_OFFICIAL)

# COMMAND ----------


