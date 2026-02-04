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

