# Databricks notebook source
# This notebook is not part of the main pipeline. It prepares the staging environment that the
# "staging" target of databricks.yml runs against: a sample of the Documents folder copied into
# the Workspace folder of the volume, and an empty _ingestion_metadata table named with the
# staging prefix in the production schema. Production files and tables are only read, never modified.
#
# 1. Keep the three settings in the next cell in sync with the staging target of databricks.yml,
#    and list the folders to copy.
# 2. Run all cells. Re-running only copies the folders that are not in the sample yet.
# 3. Deploy and run the staging job:
#      databricks bundle deploy -t staging --var="git_branch=<branch to test>"
#      databricks bundle run -t staging GLD_pipeline
# To start over from an empty staging environment, run the RESET cell at the bottom.

# COMMAND ----------

# MAGIC %run "../helpers/config"

# COMMAND ----------

# MAGIC %run "../helpers/delta_identification"

# COMMAND ----------

# Staging settings: keep in sync with the "staging" target of databricks.yml
STAGING_PREFIX   <- "staging_"
STAGING_ROOT     <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/staging/Documents"
STAGING_JSON_DIR <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/staging/json_to_publish"

# Folders to copy from the production Documents folder, relative to its root. Each entry is a
# dataset folder (all its versions are copied) or a whole country folder, e.g.
#   sample_folders <- c("THA/THA_2021_LFS", "ZAF/ZAF_2019_QLFS", "BOL")
# A useful sample has: a dataset with two version folders (overwrite and re-stacking), a quarterly
# survey, a Confidential one (the OUO table differs), one with subnatid*/gaul_* columns and a
# GLD-Light one. Keep the dta files under 900MB unless chunked ingestion is what you want to test.
sample_folders <- character(0)

# COMMAND ----------

library(dplyr)
library(sparklyr)
library(DBI)

# Run this notebook without GLD_* overrides, so that config points at production
stopifnot(
  "ROOT_DIR must be the production Documents folder" = ROOT_DIR != STAGING_ROOT,
  "TABLE_PREFIX must be empty (production)" = TABLE_PREFIX == "",
  "STAGING_PREFIX must not be empty" = nzchar(STAGING_PREFIX)
)
if (length(sample_folders) == 0) stop("No sample folders provided")

STAGING_METADATA <- paste0(TARGET_SCHEMA, ".", STAGING_PREFIX, "_ingestion_metadata")
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# 1. Copy the sample folders and survey-metadata.xlsx, keeping the folder structure
dbutils.fs.mkdirs(STAGING_ROOT)
dbutils.fs.mkdirs(STAGING_JSON_DIR)

for (folder in sample_folders) {
  src <- file.path(ROOT_DIR, folder)
  dst <- file.path(STAGING_ROOT, folder)
  if (!dir.exists(src)) stop("Not found in production: ", src)
  if (dir.exists(dst)) {
    message("Already in the sample, skipping: ", folder)
    next
  }
  message("Copying: ", folder)
  dbutils.fs.cp(src, dst, TRUE)  # TRUE = recursive
}

survey_xlsx <- "survey-metadata.xlsx"
dbutils.fs.cp(file.path(ROOT_DIR, survey_xlsx), file.path(STAGING_ROOT, survey_xlsx))

# COMMAND ----------

# 2. Check that the crawler used by delta_identification sees the sample
sample_versions <- identify_all_versions(STAGING_ROOT)
if (nrow(sample_versions) == 0) stop("No Data/Harmonized dta files found under ", STAGING_ROOT)
print(sample_versions %>% select(filename, country, year, survey, quarter, M_version, A_version))

# COMMAND ----------

# 3. Create the empty staging _ingestion_metadata table with the production structure
DBI::dbExecute(sc, paste0(
  "CREATE TABLE IF NOT EXISTS ", STAGING_METADATA,
  " AS SELECT * FROM ", METADATA_TABLE, " WHERE 1 = 0"
))
n_rows <- DBI::dbGetQuery(sc, paste0("SELECT COUNT(*) AS n FROM ", STAGING_METADATA))$n
message(STAGING_METADATA, " ready with ", n_rows, " rows")

# COMMAND ----------

# 4. Current state of the staging environment (only the metadata table before the first run)
list_staging_tables <- function() {
  DBI::dbGetQuery(sc, sprintf("SHOW TABLES IN %s LIKE '%s*'", TARGET_SCHEMA, STAGING_PREFIX))$tableName
}
print(list_staging_tables())

print(DBI::dbGetQuery(sc, paste0("
  SELECT filename, ingested, classification, stacking,
         stacked_all_table_version, stacked_ouo_table_version, published
  FROM ", STAGING_METADATA, "
  ORDER BY filename
")))

# COMMAND ----------

# RESET: uncomment to start over from an empty staging environment. It drops every table of the
# schema whose name starts with the prefix (metadata, survey tables, harmonized and availability
# tables) and the staging json files; the sample files are kept. Re-run the cells above afterwards.
# To re-test a single stage instead, flip the flags of the rows concerned, e.g.
#   UPDATE <schema>.staging__ingestion_metadata SET ingested = FALSE WHERE filename = '...'
#   UPDATE <schema>.staging__ingestion_metadata SET stacked_all_table_version = NULL, stacked_ouo_table_version = NULL WHERE ...
#   UPDATE <schema>.staging__ingestion_metadata SET published = FALSE WHERE ...
# for (t in list_staging_tables()) {
#   message("Dropping ", t)
#   DBI::dbExecute(sc, sprintf("DROP TABLE IF EXISTS %s.%s", TARGET_SCHEMA, t))
# }
# for (f in list.files(STAGING_JSON_DIR, pattern = "\\.json$", full.names = TRUE)) file.remove(f)
