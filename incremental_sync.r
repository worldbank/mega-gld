# Databricks notebook source
# This script processes and stacks GLD harmonized tables
library(sparklyr)
library(dplyr)
library(DBI)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/stacking_schema"
# MAGIC

# COMMAND ----------

# MAGIC %run "./helpers/stacking_functions"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

if (!is_databricks()) {
  source("helpers/stacking_schema.r")
  source("helpers/stacking_functions.r")
}

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# Configuration
OFFICIAL_CLASS     <- "Official Use"
CONFIDENTIAL_CLASS <- "Confidential"

# Get schema
schema <- get_gld_schema()
expected_cols <- names(schema)

# COMMAND ----------

# Test
METADATA_TABLE <- paste0(TARGET_SCHEMA, ".test_ingestion_metadata")
HARMONIZED_ALL <- paste0(TARGET_SCHEMA, ".gld_harmonized_all_test")
HARMONIZED_OFFICIAL <- paste0(TARGET_SCHEMA, ".gld_harmonized_ouo_test")

# COMMAND ----------

# Identify which country/year/survey to update
t_step <- Sys.time()
metadata <- tbl(sc, METADATA_TABLE)
validate_metadata_inputs(metadata %>% collect(), "metadata_read")
change_keys <- identify_changes(metadata)

num_changes <- validate_change_detection(change_keys)
if (num_changes == 0) {
  dbutils.notebook.exit(("No changes detected in metadata; execution halted: no updates to process."))
}
message(sprintf(">> Identify changes: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Get or create existing harmonized tables
t_step <- Sys.time()

columns_sql <- paste(
  names(schema),
  toupper(unlist(schema)),
  collapse = ", "
)

if (SparkR::tableExists(HARMONIZED_ALL)) {
  harmonized_all <- tbl(sc, HARMONIZED_ALL)
} else {
  create_query <- paste0(
    "CREATE TABLE ", HARMONIZED_ALL,
    " (", columns_sql, ") USING DELTA"
  )
  DBI::dbExecute(sc, create_query)
  harmonized_all <- tbl(sc, HARMONIZED_ALL)
}

if (SparkR::tableExists(HARMONIZED_OFFICIAL)) {
  harmonized_ouo <- tbl(sc, HARMONIZED_OFFICIAL)
} else {
  create_query <- paste0(
    "CREATE TABLE ", HARMONIZED_OFFICIAL,
    " (", columns_sql, ") USING DELTA"
  )
  DBI::dbExecute(sc, create_query)
  harmonized_ouo <- tbl(sc, HARMONIZED_OFFICIAL)
}
message(sprintf(">> Get/create harmonized tables: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Remove records that will be updated using anti-join
t_step <- Sys.time()
harmonized_all_cleaned <- harmonized_all %>%
  anti_join(
    change_keys %>% select(countrycode, year, survname, quarter),
    by = c("countrycode", "year", "survname", "quarter")
  )%>%
  select(all_of(expected_cols))

harmonized_ouo_cleaned <- harmonized_ouo %>%
  anti_join(
    change_keys %>% select(countrycode, year, survname, quarter),
    by = c("countrycode", "year", "survname", "quarter")
  )%>%
  select(all_of(expected_cols))
message(sprintf(">> Anti-join cleanup: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Validate records were removed correctly
t_step <- Sys.time()
validate_record_removal(harmonized_all, harmonized_all_cleaned, change_keys, "HARMONIZED_ALL")
validate_record_removal(harmonized_ouo, harmonized_ouo_cleaned, change_keys, "HARMONIZED_OFFICIAL")
message("✓ Verified no overlapping records remain in cleaned data")
message(sprintf(">> Validate record removal: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Process tables that need updating
t_step <- Sys.time()
update_list <- build_update_list(change_keys)

all_dfs <- list()
ouo_dfs <- list()

for (i in seq_along(update_list)) {
  item <- update_list[[i]]

  tbl_name <- item$table_name
  classification <- item$classification
  country_val <- item$country
  year_val <- item$year
  survey_val <- item$survname
  quarter_val <- item$quarter

  t_item <- Sys.time()
  message(sprintf("Processing: %s", tbl_name))

  src_df <- tbl(sc, paste0(TARGET_SCHEMA, ".", tbl_name))

  result <- align_dataframe_to_schema(src_df, schema, country_val, survey_val, quarter_val)
  aligned_df <- result$aligned_df
  extra_cols <- result$extra_cols

  if (length(extra_cols) > 0) {
    message(sprintf(
      "Extra columns ignored: %s for %s %s",
      paste(extra_cols, collapse = ", "),
      country_val,
      year_val
    ))
  }

  all_dfs[[length(all_dfs) + 1]] <- aligned_df
  if (!is.na(classification) && classification != CONFIDENTIAL_CLASS) {
    ouo_dfs[[length(ouo_dfs) + 1]] <- aligned_df
  }
  message(sprintf(">> Processed %s: %.1f sec", tbl_name, difftime(Sys.time(), t_item, units = "secs")))
}
message(sprintf(">> Total processing loop: %.1f sec (%d tables)", difftime(Sys.time(), t_step, units = "secs"), length(update_list)))

# COMMAND ----------

# Validate processing count
validate_processing_count(length(all_dfs), update_list)

# COMMAND ----------

# Batched write HARMONIZED_ALL
t_step <- Sys.time()
batched_write_table(all_dfs, harmonized_all_cleaned, HARMONIZED_ALL, sc)
message(sprintf(">> Write HARMONIZED_ALL: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Batched write HARMONIZED_OFFICIAL
t_step <- Sys.time()
batched_write_table(ouo_dfs, harmonized_ouo_cleaned, HARMONIZED_OFFICIAL, sc)
message(sprintf(">> Write HARMONIZED_OFFICIAL: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Update metadata with new stacked versions
t_step <- Sys.time()
metadata_final <- update_metadata_versions(metadata, change_keys,
                                          HARMONIZED_ALL, HARMONIZED_OFFICIAL, sc)
validate_metadata_inputs(metadata_final %>% collect(), "metadata_write")
spark_write_table(metadata_final, METADATA_TABLE, mode = "overwrite")
message(sprintf(">> Update metadata: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

# Validate metadata sync
t_step <- Sys.time()
validate_metadata_sync(METADATA_TABLE, change_keys, HARMONIZED_ALL, HARMONIZED_OFFICIAL, sc)
message(sprintf(">> Validate metadata sync: %.1f sec", difftime(Sys.time(), t_step, units = "secs")))

# COMMAND ----------

message(sprintf("\n== Total elapsed: %.1f sec ==", difftime(Sys.time(), t0, units = "secs")))
message("Stacking process completed successfully!")
