# Databricks notebook source
# This test is designed to be executed from the 'testthat_run' file. Please adjust the relative path if running this notebook standalone.

suppressPackageStartupMessages({
  library(testthat)
  library(sparklyr)
  library(dplyr)
  library(DBI)
})

# COMMAND ----------

source("../helpers/stacking_functions.r")
source("../helpers/stacking_schema.r")

# COMMAND ----------

# Load functions if not in Databricks
if (!exists("identify_changes")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  old_wd <- setwd(repo_root)
  on.exit(setwd(old_wd))
  source("helpers/stacking_schema.r")  # Load schema first
  source("helpers/stacking_functions.r")
}

# COMMAND ----------

# Setup Spark connection and test configuration
sc <- spark_connect(method = "databricks")

# Use simple names for temporary test tables (not catalog.schema.table)
TEST_METADATA <- "test_stacking_metadata"
TEST_HARMONIZED_ALL <- "test_stacking_harmonized_all"
TEST_HARMONIZED_OUO <- "test_stacking_harmonized_ouo"
TEST_SOURCE_2020 <- "test_stacking_2020_lfs"
TEST_SOURCE_2021 <- "test_stacking_2021_lfs"

# COMMAND ----------

# Helper function to create test tables
setup_test_tables <- function() {
  # Create test metadata table
  test_metadata <- data.frame(
    table_name = c(
      "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
      "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"
    ),
    classification = c("Official Use", "Confidential"),
    country = c("TST", "TST"),
    year = c("2020", "2021"),
    survey = c("LFS", "LFS"),
    table_version = c(2, 1),
    stacked_all_table_version = c(1, NA_integer_),
    stacked_ouo_table_version = c(1, NA_integer_),
    stringsAsFactors = FALSE
  )
  
  copy_to(sc, test_metadata, name = TEST_METADATA, overwrite = TRUE)
  
  # Create source tables with test data
  source_data_2020 <- data.frame(
    hhid = c("001", "002"),
    pid = c("001-01", "002-01"),
    year = c(2020L, 2020L),
    lstatus = c(1L, 1L),
    empstat = c(1L, 2L),
    stringsAsFactors = FALSE
  )
  
  copy_to(sc, source_data_2020, name = TEST_SOURCE_2020, overwrite = TRUE)
  
  source_data_2021 <- data.frame(
    hhid = c("003", "004"),
    pid = c("003-01", "004-01"),
    year = c(2021L, 2021L),
    lstatus = c(2L, 1L),
    empstat = c(3L, 1L),
    stringsAsFactors = FALSE
  )
  
  copy_to(sc, source_data_2021, name = TEST_SOURCE_2021, overwrite = TRUE)
}

cleanup_test_tables <- function() {
  # Temporary views are automatically cleaned up, but we can explicitly remove them
  tryCatch({
    DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS ", TEST_METADATA))
    DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS ", TEST_HARMONIZED_ALL))
    DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS ", TEST_HARMONIZED_OUO))
    DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS ", TEST_SOURCE_2020))
    DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS ", TEST_SOURCE_2021))
    DBI::dbExecute(sc, "DROP VIEW IF EXISTS harmonized_all_temp")
    DBI::dbExecute(sc, "DROP VIEW IF EXISTS test_metadata_bad")
    DBI::dbExecute(sc, "DROP VIEW IF EXISTS test_change_keys")
    DBI::dbExecute(sc, "DROP VIEW IF EXISTS test_metadata_uptodate")
  }, error = function(e) {
    # Views might not exist, that's okay
  })
}

# COMMAND ----------

# =============================================================================
# Integration Test: Full stacking workflow
# =============================================================================

test_that("Full stacking workflow processes updates correctly", {
  setup_test_tables()
  
  # Get schema
  schema <- get_gld_schema()
  
  # Step 1: Identify changes
  metadata <- tbl(sc, TEST_METADATA)
  change_keys <- identify_changes(metadata)
  
  num_changes <- change_keys %>% count() %>% collect() %>% pull(n)
  expect_equal(num_changes, 2) # Both tables need updating
  
  # Step 2: Build update list
  update_list <- build_update_list(change_keys)
  expect_equal(length(update_list), 2)
  
  # Step 3: Process each table
  all_dfs <- list()
  
  for (i in seq_along(update_list)) {
    item <- update_list[[i]]
    
    # Read source table - map table names to test tables
    table_map <- list(
      "TEST_2020_LFS_V01_M_V01_A_GLD_ALL" = TEST_SOURCE_2020,
      "TEST_2021_LFS_V01_M_V01_A_GLD_ALL" = TEST_SOURCE_2021
    )
    src_df <- tbl(sc, table_map[[item$table_name]])
    
    # Align to schema
    result <- align_dataframe_to_schema(src_df, schema, item$country, item$survname)
    aligned_df <- result$aligned_df
    
    all_dfs[[i]] <- aligned_df
  }
  
  expect_equal(length(all_dfs), 2)
  
  # Step 4: Union all dataframes
  final_df <- do.call(sdf_bind_rows, all_dfs)
  
  # Verify union worked
  final_count <- final_df %>% count() %>% collect() %>% pull(n)
  expect_equal(final_count, 4) # 2 records from each table
  
  # Verify countrycode and survname were added
  final_data <- final_df %>% collect()
  expect_true(all(final_data$countrycode == "TST"))
  expect_true(all(final_data$survname == "LFS"))
  
  # Step 5: Update metadata
  metadata_updated <- update_metadata_versions(metadata, change_keys)
  metadata_collected <- metadata_updated %>% collect()
  
  # Check that versions were updated
  expect_equal(metadata_collected$stacked_all_table_version[1], 2)
  expect_equal(metadata_collected$stacked_all_table_version[2], 1)
  
  cleanup_test_tables()
})

# COMMAND ----------

test_that("Anti-join correctly removes records to be updated", {
  setup_test_tables()
  
  # Get schema
  schema <- get_gld_schema()
  
  # Create initial harmonized table with old data
  old_data <- data.frame(
    countrycode = c("TST", "TST", "OLD", "OLD"),
    year = c(2020L, 2020L, 2019L, 2019L),
    survname = c("LFS", "LFS", "LFS", "LFS"),
    hhid = c("old1", "old2", "003", "004"),
    pid = c("old1-01", "old2-01", "003-01", "004-01"),
    stringsAsFactors = FALSE
  )
  
  harmonized_all <- copy_to(sc, old_data,
                            name = "harmonized_all_temp",
                            overwrite = TRUE)
  
  # Identify changes
  metadata <- tbl(sc, TEST_METADATA)
  change_keys <- identify_changes(metadata)
  
  # Remove old records using anti-join
  harmonized_cleaned <- harmonized_all %>%
    anti_join(
      change_keys %>% select(countrycode, year, survname),
      by = c("countrycode", "year", "survname")
    )
  
  cleaned_data <- harmonized_cleaned %>% collect()
  
  # Should only have OLD 2019 records left
  expect_equal(nrow(cleaned_data), 2)
  expect_true(all(cleaned_data$countrycode == "OLD"))
  expect_true(all(cleaned_data$year == 2019))
  
  # Verify no TST 2020 or TST 2021 records remain
  expect_false(any(cleaned_data$countrycode == "TST" & cleaned_data$year == 2020))
  expect_false(any(cleaned_data$countrycode == "TST" & cleaned_data$year == 2021))
  
  cleanup_test_tables()
})

# COMMAND ----------

test_that("do.call union handles large number of dataframes", {
  setup_test_tables()
  
  schema <- get_gld_schema()
  
  # Create multiple small dataframes to simulate many tables
  df_list <- list()
  
  for (i in 1:20) {
    test_df <- data.frame(
      hhid = paste0("id", i),
      pid = paste0("id", i, "-01"),
      year = 2020L,
      stringsAsFactors = FALSE
    )
    
    spark_df <- copy_to(sc, test_df, name = paste0("temp_df_", i), overwrite = TRUE)
    
    result <- align_dataframe_to_schema(spark_df, schema, "TST", "LFS")
    df_list[[i]] <- result$aligned_df
  }
  
  # Union using do.call
  final_df <- do.call(sdf_bind_rows, df_list)
  
  # Verify all records are present
  final_count <- final_df %>% count() %>% collect() %>% pull(n)
  expect_equal(final_count, 20)
  
  # Clean up temp tables
  for (i in 1:20) {
    tryCatch({
      DBI::dbExecute(sc, paste0("DROP VIEW IF EXISTS temp_df_", i))
    }, error = function(e) {})
  }
  
  cleanup_test_tables()
})

# COMMAND ----------

test_that("Metadata sync validation catches mismatches", {
  setup_test_tables()
  
  # Create test metadata with intentional mismatch
  test_metadata_bad <- data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = 2020,
    survey = "LFS",
    table_version = 3,
    stacked_all_table_version = 2,  # Intentional mismatch
    stacked_ouo_table_version = 3,
    stringsAsFactors = FALSE
  )
  
  metadata_table_name <- "test_metadata_bad"
  copy_to(sc, test_metadata_bad, name = metadata_table_name, overwrite = TRUE)
  
  # Create change_keys
  change_keys_df <- copy_to(sc, data.frame(
    countrycode = "TST",
    year = 2020L,
    survname = "LFS",
    table_version = 3,
    stringsAsFactors = FALSE
  ), name = "test_change_keys", overwrite = TRUE)
  
  # Validation should fail
  expect_error(
    validate_metadata_sync(metadata_table_name, change_keys_df, sc),
    "Metadata synchronization failed"
  )
  
  cleanup_test_tables()
})

# COMMAND ----------

test_that("Empty update list is handled correctly", {
  setup_test_tables()
  
  # Set all metadata to up-to-date
  test_metadata_uptodate <- data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 2,
    stacked_all_table_version = 2,
    stacked_ouo_table_version = 2,
    stringsAsFactors = FALSE
  )
  
  metadata <- copy_to(sc, test_metadata_uptodate, name = "test_metadata_uptodate", overwrite = TRUE)
  
  # Identify changes (should be none)
  change_keys <- identify_changes(metadata)
  num_changes <- change_keys %>% count() %>% collect() %>% pull(n)
  
  expect_equal(num_changes, 0)
  
  # Build update list (should be empty)
  update_list <- build_update_list(change_keys)
  expect_equal(length(update_list), 0)
  
  cleanup_test_tables()
})

# COMMAND ----------

message("All stacking integration tests completed!")
