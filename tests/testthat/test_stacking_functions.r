# Databricks notebook source
# This test is designed to be executed from the 'testthat_run' file. Please adjust the relative path if running this notebook standalone.

suppressPackageStartupMessages({
  library(testthat)
  library(sparklyr)
  library(dplyr)
})

# COMMAND ----------

source("../helpers/stacking_functions.r")
source("../helpers/stacking_schema.r")

# COMMAND ----------

# Setup Spark connection for tests
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# =============================================================================
# Test identify_changes function
# =============================================================================

test_that("identify_changes detects new tables (NULL stacked versions)", {
  # Create test metadata with new table
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 1,
    stacked_all_table_version = NA_integer_,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  changes <- identify_changes(test_metadata) %>% collect()
  
  expect_equal(nrow(changes), 1)
  expect_equal(changes$table_name, "TEST_2020_LFS_V01_M_V01_A_GLD_ALL")
  expect_equal(changes$countrycode, "TST")
  expect_equal(changes$year, 2020)
  expect_equal(changes$survname, "LFS")
  expect_equal(changes$table_version, 1)
})

test_that("identify_changes detects updated tables (higher table_version)", {
  # Create test metadata with updated table
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 3,
    stacked_all_table_version = 2,
    stacked_ouo_table_version = 2,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  changes <- identify_changes(test_metadata) %>% collect()
  
  expect_equal(nrow(changes), 1)
  expect_equal(changes$table_version, 3)
  expect_equal(changes$stacked_all_table_version, 2)
})

test_that("identify_changes ignores up-to-date tables", {
  # Create test metadata with up-to-date table
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    table_version = 2,
    stacked_all_table_version = 2,
    stacked_ouo_table_version = 2,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  changes <- identify_changes(test_metadata) %>% collect()
  
  expect_equal(nrow(changes), 0)
})

test_that("identify_changes uses maximum of stacked versions", {
  # Create test metadata where one stacked version is higher
  test_metadata <- copy_to(sc, data.frame(
    table_name = c("TEST_2020_LFS_V01_M_V01_A_GLD_ALL", "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"),
    classification = c("Official Use", "Official Use"),
    country = c("TST", "TST"),
    year = c("2020", "2021"),
    survey = c("LFS", "LFS"),
    table_version = c(3, 3),
    stacked_all_table_version = c(2, 1),
    stacked_ouo_table_version = c(1, 2),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  changes <- identify_changes(test_metadata) %>% collect()
  
  # Both should be detected since table_version (3) > max(stacked) (2)
  expect_equal(nrow(changes), 2)
})

# COMMAND ----------

# =============================================================================
# Test build_update_list function
# =============================================================================

test_that("build_update_list creates proper list structure", {
  # Create test change_keys
  test_changes <- copy_to(sc, data.frame(
    table_name = c("TEST_2020_LFS_V01_M_V01_A_GLD_ALL", "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"),
    classification = c("Official Use", "Confidential"),
    countrycode = c("TST", "TST"),
    year = c(2020L, 2021L),
    survname = c("LFS", "LFS"),
    table_version = c(1, 2),
    stacked_all_table_version = c(NA_integer_, 1),
    stacked_ouo_table_version = c(NA_integer_, 1),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  update_list <- build_update_list(test_changes)
  
  expect_equal(length(update_list), 2)
  
  # Check first item
  expect_equal(update_list[[1]]$table_name, "TEST_2020_LFS_V01_M_V01_A_GLD_ALL")
  expect_equal(update_list[[1]]$classification, "Official Use")
  expect_equal(update_list[[1]]$country, "TST")
  expect_equal(update_list[[1]]$year, 2020)
  expect_equal(update_list[[1]]$survname, "LFS")
  
  # Check second item
  expect_equal(update_list[[2]]$table_name, "TEST_2021_LFS_V01_M_V01_A_GLD_ALL")
})

test_that("build_update_list handles empty input", {
  # Create empty change_keys
  test_changes <- copy_to(sc, data.frame(
    table_name = character(0),
    classification = character(0),
    countrycode = character(0),
    year = integer(0),
    survname = character(0),
    table_version = integer(0),
    stacked_all_table_version = integer(0),
    stacked_ouo_table_version = integer(0),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  update_list <- build_update_list(test_changes)
  
  expect_equal(length(update_list), 0)
})

# COMMAND ----------

# =============================================================================
# Test align_dataframe_to_schema function
# =============================================================================

test_that("align_dataframe_to_schema adds countrycode and survname", {
  schema <- get_gld_schema()
  
  # Create minimal test dataframe
  test_df <- copy_to(sc, data.frame(
    year = 2020L,
    hhid = "001",
    pid = "001-01",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  aligned_df <- result$aligned_df %>% collect()
  
  expect_equal(aligned_df$countrycode[1], "TST")
  expect_equal(aligned_df$survname[1], "LFS")
})

test_that("align_dataframe_to_schema fills missing columns with NULL", {
  schema <- get_gld_schema()
  
  # Create minimal test dataframe
  test_df <- copy_to(sc, data.frame(
    hhid = "001",
    pid = "001-01",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  aligned_df <- result$aligned_df %>% collect()
  
  # Check that all schema columns exist
  schema_cols <- names(schema)
  expect_true(all(schema_cols %in% names(aligned_df)))
  
  # Check that missing columns are NULL
  expect_true(is.na(aligned_df$lstatus[1]))
  expect_true(is.na(aligned_df$empstat[1]))
})

test_that("align_dataframe_to_schema identifies extra columns", {
  schema <- get_gld_schema()
  
  # Create test dataframe with extra columns
  test_df <- copy_to(sc, data.frame(
    hhid = "001",
    pid = "001-01",
    extra_col1 = "value1",
    extra_col2 = "value2",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  extra_cols <- result$extra_cols
  
  expect_true("extra_col1" %in% extra_cols)
  expect_true("extra_col2" %in% extra_cols)
})

test_that("align_dataframe_to_schema preserves dynamic columns", {
  schema <- get_gld_schema()
  
  # Create test dataframe with dynamic columns
  test_df <- copy_to(sc, data.frame(
    hhid = "001",
    pid = "001-01",
    subnatid1 = "Region1",
    subnatid2 = "District1",
    gaul_adm1_code = "12345",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS")
  aligned_df <- result$aligned_df %>% collect()
  
  expect_true("subnatid1" %in% names(aligned_df))
  expect_true("subnatid2" %in% names(aligned_df))
  expect_true("gaul_adm1_code" %in% names(aligned_df))
  expect_equal(aligned_df$subnatid1[1], "Region1")
})

# COMMAND ----------

# =============================================================================
# Test update_metadata_versions function
# =============================================================================

test_that("update_metadata_versions updates stacked versions", {
  # Create test metadata
  test_metadata <- copy_to(sc, data.frame(
    table_name = "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
    classification = "Official Use",
    country = "TST",
    year = 2020,
    survey = "LFS",
    table_version = 1,
    stacked_all_table_version = NA_integer_,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  # Create change_keys
  test_changes <- copy_to(sc, data.frame(
    countrycode = "TST",
    year = 2020L,
    survname = "LFS",
    table_version = 3,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  updated_metadata <- update_metadata_versions(test_metadata, test_changes) %>% collect()
  
  expect_equal(updated_metadata$stacked_all_table_version[1], 3)
  expect_equal(updated_metadata$stacked_ouo_table_version[1], 3)
})

test_that("update_metadata_versions preserves unchanged records", {
  # Create test metadata with multiple records
  test_metadata <- copy_to(sc, data.frame(
    table_name = c("TEST_2020_LFS_V01_M_V01_A_GLD_ALL", "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"),
    classification = c("Official Use", "Official Use"),
    country = c("TST", "TST"),
    year = c(2020, 2021),
    survey = c("LFS", "LFS"),
    table_version = c(1, 2),
    stacked_all_table_version = c(NA_integer_, 1),
    stacked_ouo_table_version = c(NA_integer_, 1),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  # Only update first record
  test_changes <- copy_to(sc, data.frame(
    countrycode = "TST",
    year = 2020L,
    survname = "LFS",
    table_version = 3,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  updated_metadata <- update_metadata_versions(test_metadata, test_changes) %>% collect()
  
  # First record should be updated
  expect_equal(updated_metadata$stacked_all_table_version[1], 3)
  
  # Second record should be unchanged
  expect_equal(updated_metadata$stacked_all_table_version[2], 1)
  expect_equal(updated_metadata$stacked_ouo_table_version[2], 1)
})

# COMMAND ----------

# =============================================================================
# Test validation functions
# =============================================================================

test_that("validate_change_detection returns count", {
  test_changes <- copy_to(sc, data.frame(
    countrycode = c("TST", "TST"),
    year = c(2020L, 2021L),
    survname = c("LFS", "LFS"),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)
  
  count <- validate_change_detection(test_changes)
  
  expect_equal(count, 2)
})

test_that("validate_processing_count validates correctly", {
  update_list <- list(
    list(table_name = "TABLE1"),
    list(table_name = "TABLE2"),
    list(table_name = "TABLE3")
  )
  
  # All processed
  result <- validate_processing_count(3, update_list, c())
  expect_true(result)
  
  # One excluded
  result <- validate_processing_count(2, update_list, c("TABLE1"))
  expect_true(result)
  
  # Mismatch
  result <- validate_processing_count(1, update_list, c())
  expect_false(result)
})

# COMMAND ----------

message("All stacking function tests completed!")
