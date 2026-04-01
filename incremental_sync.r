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

# Connect to Spark
sc <- spark_connect(method = "databricks")


# COMMAND ----------

# Configuration
# Classification tagging
OFFICIAL_CLASS     <- "Official Use"
CONFIDENTIAL_CLASS <- "Confidential"
BATCH_SIZE <- 50

schema <- get_gld_schema()
expected_cols <- names(schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Identify Changes

# COMMAND ----------

metadata <- tbl(sc, METADATA_TABLE)
validate_metadata_inputs(metadata, "metadata_read")
change_keys <- identify_changes(metadata)

num_changes <- validate_change_detection(change_keys)

if (num_changes == 0) {
  dbutils.notebook.exit("No changes detected in metadata; execution halted: no updates to process.")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Initialize/Load Harmonized Tables

# COMMAND ----------

# Build the column definitions for table creation if needed
columns_sql <- paste(
  names(schema),
  toupper(unlist(schema)),
  collapse = ", "
)

# Check and create HARMONIZED_ALL if needed
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

# Check and create HARMONIZED_OFFICIAL if needed
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

message("✓ Harmonized tables initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Remove Records to Update (Anti-Join)

# COMMAND ----------

harmonized_all_count <- harmonized_all %>% count() %>% collect() %>% pull(n)
harmonized_ouo_count <- harmonized_ouo %>% count() %>% collect() %>% pull(n)

message(sprintf("HARMONIZED_ALL has %d existing records", harmonized_all_count))
message(sprintf("HARMONIZED_OFFICIAL has %d existing records", harmonized_ouo_count))

change_keys_selected <- change_keys %>% select(countrycode, year, survname, quarter)

# Skip anti-join if tables are empty (first run) since there's nothing to filter
if (harmonized_all_count == 0) {
  harmonized_all_cleaned <- harmonized_all %>% select(all_of(expected_cols))
  message("First run detected for HARMONIZED_ALL - skipping anti-join")
} else {
  harmonized_all_cleaned <- harmonized_all %>%
    anti_join(change_keys_selected, by = c("countrycode", "year", "survname", "quarter")) %>%
    #select(all_of(expected_cols))
}

if (harmonized_ouo_count == 0) {
  harmonized_ouo_cleaned <- harmonized_ouo %>% select(all_of(expected_cols))
  message("First run detected for HARMONIZED_OFFICIAL - skipping anti-join")
} else {
  harmonized_ouo_cleaned <- harmonized_ouo %>%
    anti_join(change_keys_selected, by = c("countrycode", "year", "survname", "quarter")) %>%
    #select(all_of(expected_cols))
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Validate Record Removal

# COMMAND ----------

validate_record_removal(harmonized_all, harmonized_all_cleaned, change_keys, "HARMONIZED_ALL")
validate_record_removal(harmonized_ouo, harmonized_ouo_cleaned, change_keys, "HARMONIZED_OFFICIAL")
message("✓ Verified no overlapping records remain in cleaned data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Build Update List and Align DataFrames

# COMMAND ----------

update_list <- build_update_list(change_keys)

# Collect aligned dataframes (lazy references, not materialized yet)
all_dfs <- list()
ouo_dfs <- list()
ouo_items <- list()
all_columns <- character()  # Track all columns across all DataFrames

# Process each table in the update list
for (i in seq_along(update_list)) {
  item <- update_list[[i]]

  tbl_name <- item$table_name
  classification <- item$classification
  country_val <- item$country
  year_val <- item$year
  survey_val <- item$survname
  quarter_val <- item$quarter

  message(sprintf("Processing: %s", tbl_name))

  # Read source table
  src_df <- tbl(sc, paste0(TARGET_SCHEMA, ".", tbl_name))

  # Align to schema
  result <- align_dataframe_to_schema(src_df, schema, country_val, survey_val, quarter_val)
  aligned_df <- result$aligned_df
  extra_cols <- result$extra_cols

  # Track all columns seen (for final schema)
  all_columns <- union(all_columns, colnames(aligned_df))

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
    ouo_items[[length(ouo_items) + 1]] <- item
  }
}

validate_processing_count(length(all_dfs), update_list)

# Determine final schema: base columns + dynamic columns from new data + existing table columns
# Include existing table columns to preserve any dynamic columns from previous runs
existing_cols <- union(colnames(harmonized_all_cleaned), colnames(harmonized_ouo_cleaned))
existing_dynamic <- setdiff(existing_cols, expected_cols)
existing_dynamic <- existing_dynamic[sapply(existing_dynamic, is_dynamic_column)]
all_columns <- union(all_columns, c(expected_cols, existing_dynamic))
dynamic_cols <- setdiff(all_columns, expected_cols)
final_columns <- c(expected_cols, sort(dynamic_cols))

message(sprintf("✓ Prepared %d tables for HARMONIZED_ALL, %d for HARMONIZED_OFFICIAL",
                length(all_dfs), length(ouo_dfs)))
message(sprintf("✓ Final schema has %d columns (%d base + %d dynamic)",
                length(final_columns), length(expected_cols), length(dynamic_cols)))
if (length(dynamic_cols) > 0) {
  message(sprintf("  Dynamic columns: %s", paste(dynamic_cols, collapse = ", ")))
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Write HARMONIZED_ALL Table

# COMMAND ----------

if (length(all_dfs) > 0) {
  # Pad cleaned data to match final schema (add NULL for any new dynamic columns)
  harmonized_all_padded <- pad_dataframe_columns(harmonized_all_cleaned, final_columns)

  # Overwrite with cleaned existing data first
  spark_write_table(harmonized_all_padded, HARMONIZED_ALL, mode = "overwrite", options = list("overwriteSchema" = "true"))
  message("✓ Wrote cleaned existing data to HARMONIZED_ALL")

  # Append new data in batches to avoid accumulating massive execution plans
  num_batches <- ceiling(length(all_dfs) / BATCH_SIZE)
  for (batch_idx in seq_len(num_batches)) {
    start_idx <- (batch_idx - 1) * BATCH_SIZE + 1
    end_idx <- min(batch_idx * BATCH_SIZE, length(all_dfs))
    batch_dfs <- all_dfs[start_idx:end_idx]
    batch_items <- update_list[start_idx:end_idx]

    if (length(batch_dfs) > 0) {
      batch_filenames <- sapply(batch_items, function(x) x$filename)
      message(sprintf("Batch %d/%d filenames: %s",
        batch_idx, num_batches,
        paste(batch_filenames, collapse = ", ")))

      batch_dfs_padded <- lapply(batch_dfs, pad_dataframe_columns, target_columns = final_columns)
      batch_union <- do.call(sdf_bind_rows, batch_dfs_padded)
      spark_write_table(batch_union, HARMONIZED_ALL, mode = "append")
      mark_batch_as_stacked(batch_filenames, METADATA_TABLE, sc)
      message(sprintf("✓ Appended batch %d/%d (%d tables) to %s",
                      batch_idx, num_batches, length(batch_dfs), HARMONIZED_ALL))
    }
  }
} else {
  message("No tables to write to HARMONIZED_ALL")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Write HARMONIZED_OFFICIAL Table

# COMMAND ----------

if (length(ouo_dfs) > 0) {
  # Pad cleaned data to match final schema (add NULL for any new dynamic columns)
  harmonized_ouo_padded <- pad_dataframe_columns(harmonized_ouo_cleaned, final_columns)

  # Overwrite with cleaned existing data first
  spark_write_table(harmonized_ouo_padded, HARMONIZED_OFFICIAL, mode = "overwrite", options = list("overwriteSchema" = "true"))
  message("✓ Wrote cleaned existing data to HARMONIZED_OFFICIAL")

  # Append new data in batches to avoid accumulating massive execution plans
  num_batches <- ceiling(length(ouo_dfs) / BATCH_SIZE)
  for (batch_idx in seq_len(num_batches)) {
    start_idx <- (batch_idx - 1) * BATCH_SIZE + 1
    end_idx <- min(batch_idx * BATCH_SIZE, length(ouo_dfs))
    batch_dfs <- ouo_dfs[start_idx:end_idx]
    batch_items <- ouo_items[start_idx:end_idx]

    if (length(batch_dfs) > 0) {
      batch_filenames <- sapply(batch_items, function(x) x$filename)
      message(sprintf("Batch %d/%d filenames: %s",
        batch_idx, num_batches,
        paste(batch_filenames, collapse = ", ")))

      batch_dfs_padded <- lapply(batch_dfs, pad_dataframe_columns, target_columns = final_columns)
      batch_union <- do.call(sdf_bind_rows, batch_dfs_padded)
      spark_write_table(batch_union, HARMONIZED_OFFICIAL, mode = "append")
      mark_batch_as_stacked(batch_filenames, METADATA_TABLE, sc)
      message(sprintf("✓ Appended batch %d/%d (%d tables) to %s",
                      batch_idx, num_batches, length(batch_dfs), HARMONIZED_OFFICIAL))
    }
  }
} else {
  message("No tables to write to HARMONIZED_OFFICIAL")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Update Metadata

# COMMAND ----------

metadata_final <- update_metadata_versions(metadata, change_keys,
                                          HARMONIZED_ALL, HARMONIZED_OFFICIAL, sc)
validate_metadata_inputs(metadata_final, "metadata_write")
spark_write_table(metadata_final, METADATA_TABLE, mode = "overwrite")
message("✓ Metadata updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9: Final Validation

# COMMAND ----------

validate_metadata_sync(METADATA_TABLE, change_keys, HARMONIZED_ALL, HARMONIZED_OFFICIAL, sc)
message("✓ Stacking process completed successfully!")
