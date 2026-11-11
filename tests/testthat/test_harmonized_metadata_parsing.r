# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(tibble)
  library(dplyr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/harmonized_metadata_parsing"

# COMMAND ----------

if (!exists("get_unique_vec")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source("helpers/harmonized_metadata_parsing.r")
}

# COMMAND ----------

# ---- get_unique_vec ----
test_that("get_unique_vec returns unique values", {
  tbl <- tibble(countrycode = c("ITA", "ITA", "FRA"))
  expect_equal(
    sort(get_unique_vec(tbl, "countrycode")),
    c("FRA", "ITA")
  )
})

# ---- get_year_range_chr ----
test_that("get_year_range_chr handles single year", {
  tbl <- tibble(year = c("2020", "2020"))
  expect_equal(get_year_range_chr(tbl), "2020")
})

test_that("get_year_range_chr handles year range", {
  tbl <- tibble(year = c("1954", "2020"))
  expect_equal(get_year_range_chr(tbl), "1954-2020")
})

test_that("get_year_range_chr returns empty string if all NA", {
  tbl <- tibble(year = NA_character_)
  expect_equal(get_year_range_chr(tbl), "")
})

# ---- get_version ----
test_that("get_version increments last version", {
  tbl_metadata <- tibble(
    table_name = "gld_harmonized_all",
    stacked_all_published_version = c("1", "2")
  )

  expect_equal(get_version(tbl_metadata, "gld_harmonized_all"), 3)
})

test_that("get_version returns 1 if no prior versions", {
  tbl_metadata <- tibble(
    table_name = "gld_harmonized_all",
    stacked_all_published_version = NA_character_
  )

  expect_equal(get_version(tbl_metadata, "gld_harmonized_all"), 1)
})

test_that("get_version_label queries the configured table for each harmonized dataset", {
  queried_tables <- character()
  local_mocked_bindings(
    sdf_sql = function(sc, sql, ...) {
      queried_tables <<- c(queried_tables, sql)
      tibble(version = c(2L, 3L))
    },
    .package = "sparklyr"
  )

  version_label <- get_version_label
  environment(version_label) <- list2env(
    list(
      HARMONIZED_ALL = "test_csc.sgld48.gld_harmonized_all",
      HARMONIZED_OFFICIAL = "test_ouo.sgld48.gld_harmonized_ouo"
    ),
    parent = environment(get_version_label)
  )
  tbl_metadata <- tibble(
    table_name = c("gld_harmonized_all", "gld_harmonized_ouo"),
    country = c("ITA", "FRA"),
    year = c("2020", "2021"),
    stacked_all_published_table_version = c(2L, 99L),
    stacked_ouo_published_table_version = c(99L, 2L),
    stacked_all_table_version = c(3L, 99L),
    stacked_ouo_table_version = c(99L, 3L)
  )

  expect_equal(
    version_label(NULL, tbl_metadata, "gld_harmonized_all"),
    "Updated data for ITA 2020"
  )
  expect_equal(
    version_label(NULL, tbl_metadata, "gld_harmonized_ouo"),
    "Updated data for FRA 2021"
  )
  expect_equal(queried_tables, c(
    "DESCRIBE HISTORY test_csc.sgld48.gld_harmonized_all",
    "DESCRIBE HISTORY test_ouo.sgld48.gld_harmonized_ouo"
  ))
})

# ---- build_update_label ----
test_that("build_update_label formats multiple countries correctly", {
  updates_df <- tibble(
    country = c("ITA", "ITA", "FRA"),
    year = c("2020", "2021", "2019")
  )

  expect_equal(
    build_update_label(updates_df),
    "Updated data for FRA 2019; ITA 2020, 2021"
  )
})

test_that("build_update_label returns empty string for empty df", {
  expect_equal(build_update_label(tibble()), "")
})

