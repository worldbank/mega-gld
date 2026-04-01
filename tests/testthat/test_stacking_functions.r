# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/stacking_functions"

# COMMAND ----------

# MAGIC %run "../../helpers/stacking_schema"

# COMMAND ----------

# MAGIC %run "../../helpers/config"

# COMMAND ----------

if (!is_databricks()) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "stacking_functions.r"))
  source(file.path(repo_root, "helpers", "stacking_schema.r"))
  source(file.path(repo_root, "helpers", "config.r"))
}

# COMMAND ----------

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")
}

# COMMAND ----------

# Most tests use plain R data frames and run anywhere (CI, local, Databricks).
# Tests for align_dataframe_to_schema require a live Spark connection and are
# skipped automatically when one is not available.

# Define the constant used inside identify_changes
OFFICIAL_CLASS <- "Official Use"

# Helper: temporarily replace get_delta_table_version with a stub that returns
# a fixed version number, then restores the original on exit.
with_mocked_delta_version <- function(version, expr) {
  original <- get("get_delta_table_version", envir = .GlobalEnv)
  assign("get_delta_table_version",
         function(table_name, sc) as.integer(version),
         envir = .GlobalEnv)
  on.exit(assign("get_delta_table_version", original, envir = .GlobalEnv))
  force(expr)
}

# COMMAND ----------

# =============================================================================
# Test validate_metadata_inputs
# =============================================================================

test_that("validate_metadata_inputs stops on NA quarter values", {
  df <- data.frame(quarter = c(NA, "Q1", NA, "Q2"), stringsAsFactors = FALSE)
  expect_error(
    validate_metadata_inputs(df, "test"),
    "column 'quarter' contains NA values"
  )
})

test_that("validate_metadata_inputs passes with valid string quarter", {
  df <- data.frame(quarter = c("Q1", "Q2", "NA"), stringsAsFactors = FALSE)
  expect_silent(validate_metadata_inputs(df, "test"))
})

# COMMAND ----------

# =============================================================================
# Test identify_changes function
# =============================================================================

test_that("identify_changes detects new tables (NULL stacked_all_table_version)", {
  # Case 1: stacked_all_table_version is NA => table has never been stacked
  test_metadata <- data.frame(
    filename = "TEST_2020_LFS_V01_M_V01_A_GLD",
    table_name = "test_2020_lfs",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    quarter = "NA",
    table_version = 1,
    stacking = 1,
    stacked_all_table_version = NA_integer_,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  expect_equal(nrow(changes), 1)
  expect_equal(changes$table_name, "test_2020_lfs")
  expect_equal(changes$countrycode, "TST")
  expect_equal(changes$year, 2020L)
  expect_equal(changes$survname, "LFS")
  expect_equal(changes$quarter, "NA")
  expect_equal(changes$table_version, 1)
})

test_that("identify_changes detects Official Use tables with missing OUO version", {
  # Case 2: stacked_all is set but stacked_ouo is NA for an Official Use survey
  test_metadata <- data.frame(
    filename = "TEST_2020_LFS_V01_M_V01_A_GLD",
    table_name = "test_2020_lfs",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    quarter = "NA",
    table_version = 3,
    stacking = 1,
    stacked_all_table_version = 3L,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  expect_equal(nrow(changes), 1)
  expect_equal(changes$table_name, "test_2020_lfs")
})

test_that("identify_changes ignores fully up-to-date tables", {
  test_metadata <- data.frame(
    filename = "TEST_2020_LFS_V01_M_V01_A_GLD",
    table_name = "test_2020_lfs",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    quarter = "NA",
    table_version = 2,
    stacking = 1,
    stacked_all_table_version = 2L,
    stacked_ouo_table_version = 2L,
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  expect_equal(nrow(changes), 0)
})

test_that("identify_changes excludes tables with stacking = 0", {
  test_metadata <- data.frame(
    filename = "TEST_2020_LFS_V01_M_V01_A_GLD",
    table_name = "test_2020_lfs",
    classification = "Official Use",
    country = "TST",
    year = "2020",
    survey = "LFS",
    quarter = "NA",
    table_version = 1,
    stacking = 0,
    stacked_all_table_version = NA_integer_,
    stacked_ouo_table_version = NA_integer_,
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  expect_equal(nrow(changes), 0)
})

test_that("identify_changes handles both Case 1 and Case 2 simultaneously", {
  # Row 1 (2020): ALL is set, OUO is NA, Official Use => Case 2 fires
  # Row 2 (2021): ALL is NA                            => Case 1 fires
  test_metadata <- data.frame(
    filename = c(
      "TEST_2020_LFS_V01_M_V01_A_GLD",
      "TEST_2021_LFS_V01_M_V01_A_GLD"
    ),
    table_name = c(
      "test_2020_lfs",
      "test_2021_lfs"
    ),
    classification = c("Official Use", "Official Use"),
    country = c("TST", "TST"),
    year = c("2020", "2021"),
    survey = c("LFS", "LFS"),
    quarter = c("NA", "NA"),
    table_version = c(3, 3),
    stacking = c(1, 1),
    stacked_all_table_version = c(2L, NA_integer_),
    stacked_ouo_table_version = c(NA_integer_, NA_integer_),
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  expect_equal(nrow(changes), 2)
})

test_that("identify_changes handles quarterly surveys correctly", {
  # Two quarterly surveys for the same country-year but different quarters
  test_metadata <- data.frame(
    filename = c(
      "TEST_2020_LFS-Q1_V01_M_V01_A_GLD",
      "TEST_2020_LFS-Q2_V01_M_V01_A_GLD"
    ),
    table_name = c(
      "test_2020_lfs_q1",
      "test_2020_lfs_q2"
    ),
    classification = c("Official Use", "Official Use"),
    country = c("TST", "TST"),
    year = c("2020", "2020"),
    survey = c("LFS", "LFS"),
    quarter = c("Q1", "Q2"),
    table_version = c(1, 1),
    stacking = c(1, 1),
    stacked_all_table_version = c(NA_integer_, NA_integer_),
    stacked_ouo_table_version = c(NA_integer_, NA_integer_),
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  expect_equal(nrow(changes), 2)
  expect_true("Q1" %in% changes$quarter)
  expect_true("Q2" %in% changes$quarter)
})

test_that("identify_changes picks up annual and quarterly updates together", {
  # Mixed fixture: annual + Q1 need stacking, Q2 is already up to date
  test_metadata <- data.frame(
    filename = c(
      "TST_2020_LFS_V01_M_V01_A_GLD",
      "TST_2020_LFS-Q1_V01_M_V01_A_GLD",
      "TST_2020_LFS-Q2_V01_M_V01_A_GLD"
    ),
    table_name = c(
      "test_2020_lfs",
      "test_2020_lfs_q1",
      "test_2020_lfs_q2"
    ),
    classification = c("Official Use", "Official Use", "Official Use"),
    country = c("TST", "TST", "TST"),
    year = c("2020", "2020", "2020"),
    survey = c("LFS", "LFS", "LFS"),
    quarter = c("NA", "Q1", "Q2"),
    table_version = c(1, 1, 1),
    stacking = c(1, 1, 1),
    stacked_all_table_version = c(NA_integer_, NA_integer_, 1L),
    stacked_ouo_table_version = c(NA_integer_, NA_integer_, 1L),
    stringsAsFactors = FALSE
  )

  changes <- identify_changes(test_metadata)

  # Annual and Q1 should be picked up, Q2 should not
  expect_equal(nrow(changes), 2)
  expect_true("NA" %in% changes$quarter)
  expect_true("Q1" %in% changes$quarter)
  expect_false("Q2" %in% changes$quarter)
})

# COMMAND ----------

# =============================================================================
# Test build_update_list function
# =============================================================================

test_that("build_update_list creates proper list structure", {
  test_changes <- data.frame(
    table_name = c(
      "TEST_2020_LFS_V01_M_V01_A_GLD_ALL",
      "TEST_2021_LFS_V01_M_V01_A_GLD_ALL"
    ),
    classification = c("Official Use", "Confidential"),
    countrycode = c("TST", "TST"),
    year = c(2020L, 2021L),
    survname = c("LFS", "LFS"),
    quarter = c("NA", "NA"),
    table_version = c(1, 2),
    stacked_all_table_version = c(NA_integer_, 1L),
    stacked_ouo_table_version = c(NA_integer_, 1L),
    stringsAsFactors = FALSE
  )

  update_list <- build_update_list(test_changes)

  expect_equal(length(update_list), 2)

  expect_equal(update_list[[1]]$table_name, "TEST_2020_LFS_V01_M_V01_A_GLD_ALL")
  expect_equal(update_list[[1]]$classification, "Official Use")
  expect_equal(update_list[[1]]$country, "TST")
  expect_equal(update_list[[1]]$year, 2020L)
  expect_equal(update_list[[1]]$survname, "LFS")
  expect_equal(update_list[[1]]$quarter, "NA")

  expect_equal(update_list[[2]]$table_name, "TEST_2021_LFS_V01_M_V01_A_GLD_ALL")
})

test_that("build_update_list includes quarter for quarterly surveys", {
  test_changes <- data.frame(
    table_name = c(
      "TEST_2020_LFS-Q1_V01_M_V01_A_GLD_ALL",
      "TEST_2020_LFS-Q2_V01_M_V01_A_GLD_ALL"
    ),
    classification = c("Official Use", "Official Use"),
    countrycode = c("TST", "TST"),
    year = c(2020L, 2020L),
    survname = c("LFS", "LFS"),
    quarter = c("Q1", "Q2"),
    table_version = c(1, 1),
    stacked_all_table_version = c(NA_integer_, NA_integer_),
    stacked_ouo_table_version = c(NA_integer_, NA_integer_),
    stringsAsFactors = FALSE
  )

  update_list <- build_update_list(test_changes)

  expect_equal(length(update_list), 2)
  expect_equal(update_list[[1]]$quarter, "Q1")
  expect_equal(update_list[[2]]$quarter, "Q2")
})

test_that("build_update_list handles empty input", {
  test_changes <- data.frame(
    table_name = character(0),
    classification = character(0),
    countrycode = character(0),
    year = integer(0),
    survname = character(0),
    quarter = character(0),
    table_version = integer(0),
    stacked_all_table_version = integer(0),
    stacked_ouo_table_version = integer(0),
    stringsAsFactors = FALSE
  )

  update_list <- build_update_list(test_changes)

  expect_equal(length(update_list), 0)
})

# COMMAND ----------

# =============================================================================
# Test align_dataframe_to_schema function
# These tests require a live Spark connection (sc) and are skipped in CI.
# =============================================================================

test_that("align_dataframe_to_schema adds countrycode, survname, and quarter", {
  skip_if(!exists("sc"), "Spark connection (sc) required")
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    year = 2020L, hhid = "001", pid = "001-01",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS", "Q1")
  aligned_df <- result$aligned_df %>% collect()

  expect_equal(aligned_df$countrycode[1], "TST")
  expect_equal(aligned_df$survname[1], "LFS")
  expect_equal(aligned_df$quarter[1], "Q1")
})

test_that("align_dataframe_to_schema fills missing columns with NULL", {
  skip_if(!exists("sc"), "Spark connection (sc) required")
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    hhid = "001", pid = "001-01",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS", "NA")
  aligned_df <- result$aligned_df %>% collect()

  expect_true(all(names(schema) %in% names(aligned_df)))
  expect_true(is.na(aligned_df$lstatus[1]))
  expect_true(is.na(aligned_df$empstat[1]))
})

test_that("align_dataframe_to_schema identifies extra columns", {
  skip_if(!exists("sc"), "Spark connection (sc) required")
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    hhid = "001", pid = "001-01",
    extra_col1 = "value1", extra_col2 = "value2",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS", "NA")

  expect_true("extra_col1" %in% result$extra_cols)
  expect_true("extra_col2" %in% result$extra_cols)
})

test_that("align_dataframe_to_schema preserves dynamic columns", {
  skip_if(!exists("sc"), "Spark connection (sc) required")
  schema <- get_gld_schema()

  test_df <- copy_to(sc, data.frame(
    hhid = "001", pid = "001-01",
    subnatid1 = "Region1", subnatid2 = "District1", gaul_adm1_code = "12345",
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  result <- align_dataframe_to_schema(test_df, schema, "TST", "LFS", "NA")
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

test_that("update_metadata_versions only updates the stacking=1 filename, leaves others untouched", {
  with_mocked_delta_version(54, {
    test_metadata <- data.frame(
      filename = c(
        "ARM_2015_LFS_V01_M_V01_A_GLD",
        "ARM_2015_LFS_V01_M_V02_A_GLD",
        "ARM_2015_LFS_V01_M_V03_A_GLD",
        "ARM_2015_LFS_V01_M_V04_A_GLD"
      ),
      table_name = rep("arm_2015_lfs", 4),
      classification = rep("Official Use", 4),
      country = rep("ARM", 4),
      year = rep(2015L, 4),
      survey = rep("LFS", 4),
      quarter = rep("NA", 4),
      table_version = c(1L, 2L, 3L, 4L),
      stacked_all_table_version = rep(NA_integer_, 4),
      stacked_ouo_table_version = rep(NA_integer_, 4),
      stringsAsFactors = FALSE
    )

    test_changes <- data.frame(
      filename = "ARM_2015_LFS_V01_M_V04_A_GLD",
      countrycode = "ARM",
      year = 2015L,
      survname = "LFS",
      quarter = "NA",
      table_version = 4L,
      stringsAsFactors = FALSE
    )

    updated <- update_metadata_versions(
      test_metadata, test_changes,
      "dummy_all_table", "dummy_ouo_table", sc = NULL
    )

    row_v04 <- updated[updated$filename == "ARM_2015_LFS_V01_M_V04_A_GLD", ]
    expect_equal(row_v04$stacked_all_table_version, 54L)
    expect_equal(row_v04$stacked_ouo_table_version, 54L)

    rows_v01_v03 <- updated[updated$filename != "ARM_2015_LFS_V01_M_V04_A_GLD", ]
    expect_true(all(is.na(rows_v01_v03$stacked_all_table_version)))
    expect_true(all(is.na(rows_v01_v03$stacked_ouo_table_version)))
  })
})

test_that("update_metadata_versions updates annual and quarterly independently", {
  with_mocked_delta_version(7, {
    test_metadata <- data.frame(
      filename = c(
        "TST_2020_LFS_V01_M_V01_A_GLD",
        "TST_2020_LFS-Q1_V01_M_V01_A_GLD",
        "TST_2020_LFS-Q2_V01_M_V01_A_GLD"
      ),
      table_name = rep("tst_2020_lfs", 3),
      classification = rep("Official Use", 3),
      country = rep("TST", 3),
      year = rep(2020L, 3),
      survey = rep("LFS", 3),
      quarter = c("NA", "Q1", "Q2"),
      table_version = rep(1L, 3),
      stacked_all_table_version = c(NA_integer_, NA_integer_, 4L),
      stacked_ouo_table_version = c(NA_integer_, NA_integer_, 4L),
      stringsAsFactors = FALSE
    )

    test_changes <- data.frame(
      filename = c("TST_2020_LFS_V01_M_V01_A_GLD", "TST_2020_LFS-Q1_V01_M_V01_A_GLD"),
      countrycode = c("TST", "TST"),
      year = c(2020L, 2020L),
      survname = c("LFS", "LFS"),
      quarter = c("NA", "Q1"),
      table_version = c(1L, 1L),
      stringsAsFactors = FALSE
    )

    updated <- update_metadata_versions(
      test_metadata, test_changes,
      "dummy_all_table", "dummy_ouo_table", sc = NULL
    )

    row_annual <- updated[updated$quarter == "NA", ]
    expect_equal(row_annual$stacked_all_table_version, 7L)
    expect_equal(row_annual$stacked_ouo_table_version, 7L)

    row_q1 <- updated[updated$quarter == "Q1", ]
    expect_equal(row_q1$stacked_all_table_version, 7L)
    expect_equal(row_q1$stacked_ouo_table_version, 7L)

    row_q2 <- updated[updated$quarter == "Q2", ]
    expect_equal(row_q2$stacked_all_table_version, 4L)
    expect_equal(row_q2$stacked_ouo_table_version, 4L)
  })
})

# COMMAND ----------

# =============================================================================
# Test validation functions
# =============================================================================

test_that("validate_change_detection returns TRUE when changes are found", {
  test_changes <- data.frame(
    countrycode = c("TST", "TST"),
    year = c(2020L, 2021L),
    survname = c("LFS", "LFS"),
    quarter = c("NA", "NA"),
    stringsAsFactors = FALSE
  )

  result <- suppressMessages(validate_change_detection(test_changes))

  expect_true(result)
})

test_that("validate_change_detection returns 0L when no changes are found", {
  test_empty <- data.frame(
    countrycode = character(0),
    year = integer(0),
    survname = character(0),
    quarter = character(0),
    stringsAsFactors = FALSE
  )

  result <- suppressMessages(validate_change_detection(test_empty))

  expect_equal(result, 0L)
})

test_that("validate_change_detection allows same country-year-survey with different quarters", {
  # Same country/year/survey but different quarters should NOT be flagged as duplicates
  test_changes <- data.frame(
    countrycode = c("TST", "TST"),
    year = c(2020L, 2020L),
    survname = c("LFS", "LFS"),
    quarter = c("Q1", "Q2"),
    stringsAsFactors = FALSE
  )

  result <- suppressMessages(validate_change_detection(test_changes))

  expect_true(result)
})

test_that("validate_processing_count validates correctly", {
  update_list <- list(
    list(table_name = "TABLE1"),
    list(table_name = "TABLE2"),
    list(table_name = "TABLE3")
  )

  # Processed count matches list length => TRUE
  result <- suppressMessages(validate_processing_count(3, update_list))
  expect_true(result)

  # Processed count does not match => FALSE
  result <- suppressMessages(validate_processing_count(1, update_list))
  expect_false(result)
})

# COMMAND ----------

# =============================================================================
# Test pad_dataframe_columns function
# =============================================================================

test_that("pad_dataframe_columns returns df unchanged when all columns present", {
  test_df <- data.frame(
    countrycode = "TST",
    year = 2020L,
    survname = "LFS",
    stringsAsFactors = FALSE
  )

  target_columns <- c("countrycode", "year", "survname")
  result <- pad_dataframe_columns(test_df, target_columns)

  expect_equal(names(result), target_columns)
  expect_equal(nrow(result), 1)
})

test_that("pad_dataframe_columns adds missing columns as NA", {
  skip_if(!exists("sc"), "Spark connection (sc) required")

  test_df <- copy_to(sc, data.frame(
    countrycode = "TST",
    year = 2020L,
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  target_columns <- c("countrycode", "year", "survname", "quarter")
  result <- pad_dataframe_columns(test_df, target_columns) %>% collect()

  expect_equal(names(result), target_columns)
  expect_true(is.na(result$survname[1]))
  expect_true(is.na(result$quarter[1]))
})

test_that("pad_dataframe_columns preserves existing column values", {
  test_df <- data.frame(
    countrycode = "TST",
    year = 2020L,
    stringsAsFactors = FALSE
  )

  target_columns <- c("countrycode", "year", "survname")
  result <- pad_dataframe_columns(test_df, target_columns)

  expect_equal(result$countrycode[1], "TST")
  expect_equal(result$year[1], 2020L)
})

test_that("pad_dataframe_columns enforces column order to match target", {
  test_df <- data.frame(
    year = 2020L,
    countrycode = "TST",
    survname = "LFS",
    stringsAsFactors = FALSE
  )

  target_columns <- c("countrycode", "year", "survname")
  result <- pad_dataframe_columns(test_df, target_columns)

  expect_equal(names(result), target_columns)
})

test_that("pad_dataframe_columns handles multiple missing columns across multiple rows", {
  skip_if(!exists("sc"), "Spark connection (sc) required")

  test_df <- copy_to(sc, data.frame(
    countrycode = c("TST", "ARM"),
    year = c(2020L, 2021L),
    stringsAsFactors = FALSE
  ), overwrite = TRUE)

  target_columns <- c("countrycode", "year", "survname", "quarter")
  result <- pad_dataframe_columns(test_df, target_columns) %>% collect()

  expect_equal(nrow(result), 2)
  expect_true(all(is.na(result$survname)))
  expect_true(all(is.na(result$quarter)))
})

# COMMAND ----------

test_that("mark_batch_as_stacked sets stacked to TRUE only for specified filenames", {
  skip_if(!exists("sc"), "Spark connection (sc) required")

  test_data <- data.frame(
    filename = c(
      "ARM_2015_LFS_V01_M_V01_A_GLD",
      "ARM_2015_LFS_V01_M_V02_A_GLD",
      "ARM_2015_LFS_V01_M_V03_A_GLD",
      "ARM_2015_LFS_V01_M_V04_A_GLD"
    ),
    stacked = c(FALSE, FALSE, FALSE, FALSE),
    stringsAsFactors = FALSE
  )

  tmp_table  <- "tmp_test_mark_batch_as_stacked"
  stage_view <- "tmp_test_mark_batch_stage"

  # Write to a staging view, then materialize as a proper Delta table
  copy_to(sc, test_data, stage_view, overwrite = TRUE)
  DBI::dbExecute(sc, sprintf(
    "CREATE OR REPLACE TABLE %s USING DELTA AS SELECT * FROM %s",
    tmp_table, stage_view
  ))

  batch_filenames <- c(
    "ARM_2015_LFS_V01_M_V03_A_GLD",
    "ARM_2015_LFS_V01_M_V04_A_GLD"
  )

  mark_batch_as_stacked(batch_filenames, tmp_table, sc)

  result <- tbl(sc, tmp_table) %>% collect()

  # V03 and V04 should be TRUE
  rows_updated <- result[result$filename %in% batch_filenames, ]
  expect_true(all(rows_updated$stacked))

  # V01 and V02 should remain FALSE
  rows_unchanged <- result[!result$filename %in% batch_filenames, ]
  expect_true(all(!rows_unchanged$stacked))

  DBI::dbExecute(sc, sprintf("DROP TABLE IF EXISTS %s", tmp_table))
  DBI::dbExecute(sc, sprintf("DROP VIEW IF EXISTS %s", stage_view))
})

# COMMAND ----------

message("All stacking function tests completed!")
