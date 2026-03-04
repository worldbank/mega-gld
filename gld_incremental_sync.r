# This script processes and stacks GLD harmonized tables

library(sparklyr)
library(dplyr)
library(DBI)

# Source helper functions

source("helpers/stacking_schema.r")
source("helpers/stacking_functions.r")

# Connect to Spark
sc <- spark_connect(method = "databricks")

# Configuration
# Classification tagging
OFFICIAL_CLASS     <- "Official Use"
CONFIDENTIAL_CLASS <- "Confidential"
TARGET_SCHEMA  <- "prd_csc_mega.sgld48"
METADATA_TABLE <- paste0(TARGET_SCHEMA, ".test_ingestion_metadata")

# Table names
HARMONIZED_ALL <- paste0(TARGET_SCHEMA, ".GLD_HARMONIZED_ALL_TEST")
HARMONIZED_OFFICIAL <- paste0(TARGET_SCHEMA, ".GLD_HARMONIZED_OUO_TEST")

# Get schema
schema <- get_gld_schema()
expected_cols <- names(schema)

# ============================================================================
# Identify which country/year/survey to update
# ============================================================================

metadata <- tbl(sc, METADATA_TABLE)
change_keys <- identify_changes(metadata)

# Validation: Check if any changes were detected
num_changes <- validate_change_detection(change_keys)
if (num_changes == 0) {
  stop("No changes detected in metadata; execution halted: no updates to process.")
}

# ============================================================================
# Get or create existing harmonized tables and remove records to be updated
# ============================================================================
# ============================================================================

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

# Remove records that will be updated using anti-join
harmonized_all_cleaned <- harmonized_all %>%
  anti_join(
    change_keys %>% select(countrycode, year, survname, quarter),
    by = c("countrycode", "year", "survname", "quarter")
  )

harmonized_ouo_cleaned <- harmonized_ouo %>%
  anti_join(
    change_keys %>% select(countrycode, year, survname, quarter),
    by = c("countrycode", "year", "survname", "quarter")
  )

# ============================================================================
# Validation: Verify records were removed correctly
# ============================================================================

validate_record_removal(harmonized_all, harmonized_all_cleaned, change_keys, "HARMONIZED_ALL")
validate_record_removal(harmonized_ouo, harmonized_ouo_cleaned, change_keys, "HARMONIZED_OFFICIAL")

message("✓ Verified no overlapping records remain in cleaned data")

# ============================================================================
# Process tables that need updating
# ============================================================================

# Build update list
update_list <- build_update_list(change_keys)

# Collect new dataframes
all_dfs <- list()
ouo_dfs <- list()

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
  
  # Log extra columns
  if (length(extra_cols) > 0) {
    message(sprintf(
      "Extra columns ignored: %s for %s %s",
      paste(extra_cols, collapse = ", "),
      country_val,
      year_val
    ))
  }
  
  # Add to appropriate lists
  all_dfs[[length(all_dfs) + 1]] <- aligned_df
  if (!is.na(classification) && classification != CONFIDENTIAL_CLASS) {
    ouo_dfs[[length(ouo_dfs) + 1]] <- aligned_df
  }
}

# ============================================================================
# Validation: Check that expected tables were processed
# ============================================================================

validate_processing_count(length(all_dfs), update_list)

# Union all tables and write results
if (length(all_dfs) > 0) {
  # Add cleaned existing data to the list first
  all_dfs <- c(list(harmonized_all_cleaned), all_dfs)
  # Union all dataframes at once
  final_df <- do.call(sdf_bind_rows, all_dfs)
  # Write to table
  spark_write_table(final_df, HARMONIZED_ALL, mode = "overwrite", options = list("overwriteSchema" = "true"))
}

if (length(ouo_dfs) > 0) {
  # Add cleaned existing data to the list first
  ouo_dfs <- c(list(harmonized_ouo_cleaned), ouo_dfs)
  # Union all dataframes at once
  ouo_df <- do.call(sdf_bind_rows, ouo_dfs)
  # Write to table
  spark_write_table(ouo_df, HARMONIZED_OFFICIAL, mode = "overwrite", options = list("overwriteSchema" = "true"))
}

#
# ============================================================================
# Update metadata with new stacked versions
# ============================================================================

metadata_final <- update_metadata_versions(metadata, change_keys, 
                                          HARMONIZED_ALL, HARMONIZED_OFFICIAL, sc)
# Write updated metadata back to table
spark_write_table(metadata_final, METADATA_TABLE, mode = "overwrite")

# ============================================================================
# Validation: Verify metadata sync worked correctly
# ============================================================================

validate_metadata_sync(METADATA_TABLE, change_keys, HARMONIZED_ALL, HARMONIZED_OFFICIAL, sc)

message("Stacking process completed successfully!")
