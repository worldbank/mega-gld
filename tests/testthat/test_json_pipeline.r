# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(tibble)
  library(withr)
  library(dplyr)
  library(stringr)
  library(purrr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/json_pipeline"

# COMMAND ----------

if (!exists("fetch_countries_names")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "json_pipeline.r"))
}

# COMMAND ----------

test_that("fetch_countries_names selects and renames country columns", {
  fake_tbl <- tibble::tibble(
    country_code = c("USA", "KEN"),
    country_name = c("United States", "Kenya"),
    extra = 1:2
  )

  # Save original if it exists
  env <- environment(fetch_countries_names)
  had_tbl <- exists("tbl", envir = env, inherits = FALSE)
  if (had_tbl) old_tbl <- get("tbl", envir = env, inherits = FALSE)

  # Override tbl() for this test
  assign("tbl", function(sc, country_table) fake_tbl, envir = env)

  # Restore after test
  on.exit({
    if (had_tbl) {
      assign("tbl", old_tbl, envir = env)
    } else {
      rm("tbl", envir = env)
    }
  }, add = TRUE)

  out <- fetch_countries_names(sc = "ignored", country_table = "ignored")

  expect_s3_class(out, "data.frame")
  expect_equal(names(out), c("code", "name"))
  expect_equal(out$code, c("USA", "KEN"))
  expect_equal(out$name, c("United States", "Kenya"))
})



test_that("fetch_survey_metadata calls read_excel on root_dir/filename", {
  tmp <- tempdir()
  expected_path <- file.path(tmp, "survey-metadata.xlsx")

  env <- environment(fetch_survey_metadata)
  had <- exists("read_excel", envir = env, inherits = FALSE)
  if (had) old <- get("read_excel", envir = env, inherits = FALSE)

  assign("read_excel", function(path) {
    expect_equal(path, expected_path)
    tibble::tibble(x = 1)
  }, envir = env)

  on.exit({
    if (had) assign("read_excel", old, envir = env) else rm("read_excel", envir = env)
  }, add = TRUE)

  out <- fetch_survey_metadata(root_dir = tmp)
  expect_equal(out, tibble::tibble(x = 1))
})


test_that("fetch_survey_metadata respects custom filename", {
  tmp <- tempdir()
  expected_path <- file.path(tmp, "custom.xlsx")

  env <- environment(fetch_survey_metadata)
  had <- exists("read_excel", envir = env, inherits = FALSE)
  if (had) old <- get("read_excel", envir = env, inherits = FALSE)

  assign("read_excel", function(path) {
    expect_equal(path, expected_path)
    tibble::tibble(ok = TRUE)
  }, envir = env)

  on.exit({
    if (had) assign("read_excel", old, envir = env) else rm("read_excel", envir = env)
  }, add = TRUE)

  out <- fetch_survey_metadata(root_dir = tmp, filename = "custom.xlsx")
  expect_equal(out, tibble::tibble(ok = TRUE))
})



test_that("compute_json_inputs keeps only unpublished rows with filled classification", {
  metadata <- tibble::tibble(
    country = c("USA", "USA", "KEN", "KEN", "FRA"),
    survey  = c("LFS-foo", "LFS-bar", "DHS-2020", "DHS-2020", "EU-SILC"),
    published = c(FALSE, FALSE, FALSE, TRUE, FALSE),
    classification = c(NA, "Official Use", "   ", NA, "")
  )

  survey <- tibble::tibble(
    country = c("USA", "USA", "KEN", "FRA"),
    survey  = c("LFS-foo", "LFS-bar", "DHS-2020", "EU-SILC"),
    survey_extended = c("Labor Force Survey", "Labor Force Survey", "DHS", "EU-SILC")
  )

  valid_pairs_df <- tibble::tibble(
    country = c("USA", "KEN", "FRA"),
    survey_clean = c("LFS", "DHS", "EU"),
    ok = TRUE
  )

  out <- compute_json_inputs(metadata, survey = survey, valid_pairs_df = valid_pairs_df)

  # Kept rows should be: USA/LFS-foo (NA), KEN/DHS-2020 ("   "), FRA/EU-SILC ("")
  expect_equal(nrow(out), 3)

  expect_true(all(out$published == FALSE))
  expect_true(all(!is.na(out$classification)))
  expect_true(all(trimws(out$classification) != ""))

  # Ensure we did NOT keep any rows with a non-empty classification
  expect_false(any(trimws(out$classification) %in% c("Official Use", "Confidential")))

  # Joins happened
  expect_true("survey_extended" %in% names(out))
  expect_true("ok" %in% names(out))

  # survey_clean computed
  expect_equal(out$survey_clean, c("LFS", "DHS", "EU"))
})



test_that("compute_json_inputs drops unpublished rows when classification is non-missing", {
  metadata <- tibble::tibble(
    country = c("USA", "USA"),
    survey  = c("LFS-foo", "LFS-bar"),
    published = c(FALSE, FALSE),
    classification = c("Official Use", "Confidential")
  )

  survey <- tibble::tibble(
    country = c("USA", "USA"),
    survey  = c("LFS-foo", "LFS-bar"),
    survey_extended = c("Labor Force Survey", "Labor Force Survey")
  )

  valid_pairs_df <- tibble::tibble(
    country = "USA",
    survey_clean = "LFS",
    ok = TRUE
  )

  out <- compute_json_inputs(metadata, survey = survey, valid_pairs_df = valid_pairs_df)
  expect_equal(nrow(out), 0)
})

test_that("compute_json_inputs sets survey_clean to text before first hyphen", {
  metadata <- tibble::tibble(
    country = "USA",
    survey = "ABC-DEF-GHI",
    published = FALSE,
    classification = NA_character_
  )

  survey <- tibble::tibble(country = "USA", survey = "ABC-DEF-GHI")
  valid_pairs_df <- tibble::tibble(country = "USA", survey_clean = "ABC")

  out <- compute_json_inputs(metadata, survey = survey, valid_pairs_df = valid_pairs_df)
  expect_equal(out$survey_clean, "ABC")
})


test_that("compute_json_inputs works without survey and valid_pairs_df", {
  metadata <- tibble::tibble(
    country = c("USA", "KEN"),
    survey  = c("LFS-foo", "DHS-2020"),
    published = c(FALSE, FALSE),
    classification = c(NA, "")
  )

  out <- compute_json_inputs(metadata)

  expect_equal(nrow(out), 2)
  expect_true("survey_clean" %in% names(out))
  expect_equal(out$survey_clean, c("LFS", "DHS"))
})

test_that("compute_json_inputs works with valid_pairs_df only", {
  metadata <- tibble::tibble(
    country = c("USA", "KEN"),
    survey  = c("LFS-foo", "DHS-2020"),
    published = c(FALSE, FALSE),
    classification = c(NA, "")
  )

  valid_pairs_df <- tibble::tibble(
    country = c("USA", "KEN"),
    survey_clean = c("LFS", "DHS"),
    ok = TRUE
  )

  out <- compute_json_inputs(metadata, valid_pairs_df = valid_pairs_df)

  expect_true("ok" %in% names(out))
  expect_equal(out$ok, c(TRUE, TRUE))
})



test_that("write_json_files writes one JSON per row with expected filename", {
  df <- tibble::tibble(
    filename = c("USA_2020_LFS_V01", "KEN_2019_DHS_V01"),
    country = c("USA", "KEN")
  )
  countries <- tibble::tibble(code = c("USA", "KEN"), name = c("United States", "Kenya"))
  out_dir <- tempdir()

  calls <- list(make = 0L, write = character())

  env <- environment(write_json_files)

  # Save originals if present
  had_make <- exists("make_mdl_json", envir = env, inherits = FALSE)
  if (had_make) old_make <- get("make_mdl_json", envir = env, inherits = FALSE)

  had_write <- exists("write_json", envir = env, inherits = FALSE)
  if (had_write) old_write <- get("write_json", envir = env, inherits = FALSE)

  # Override
  assign("make_mdl_json", function(row, countries_names) {
    calls$make <<- calls$make + 1L
    list(idno = row$filename)
  }, envir = env)

  assign("write_json", function(x, path, pretty, auto_unbox) {
    calls$write <<- c(calls$write, path)
    invisible(NULL)
  }, envir = env)

  # Restore
  on.exit({
    if (had_make) assign("make_mdl_json", old_make, envir = env) else rm("make_mdl_json", envir = env)
    if (had_write) assign("write_json", old_write, envir = env) else rm("write_json", envir = env)
  }, add = TRUE)

  write_json_files(df, countries, out_dir)

  expect_equal(calls$make, 2L)
  expect_equal(
    calls$write,
    file.path(out_dir, c("DDI_USA_2020_LFS_V01_WB.json", "DDI_KEN_2019_DHS_V01_WB.json"))
  )
})


test_that("write_json_files warns and continues when make_mdl_json errors", {
  df <- tibble::tibble(
    filename = c("OK_ONE", "BAD_ONE", "OK_TWO"),
    country = c("USA", "USA", "KEN")
  )
  countries <- tibble::tibble(code = c("USA", "KEN"), name = c("United States", "Kenya"))
  out_dir <- tempdir()

  written <- character()

  env <- environment(write_json_files)

  had_make <- exists("make_mdl_json", envir = env, inherits = FALSE)
  if (had_make) old_make <- get("make_mdl_json", envir = env, inherits = FALSE)

  had_write <- exists("write_json", envir = env, inherits = FALSE)
  if (had_write) old_write <- get("write_json", envir = env, inherits = FALSE)

  assign("make_mdl_json", function(row, countries_names) {
    if (row$filename == "BAD_ONE") stop("boom")
    list(idno = row$filename)
  }, envir = env)

  assign("write_json", function(x, path, pretty, auto_unbox) {
    written <<- c(written, basename(path))
    invisible(NULL)
  }, envir = env)

  on.exit({
    if (had_make) assign("make_mdl_json", old_make, envir = env) else rm("make_mdl_json", envir = env)
    if (had_write) assign("write_json", old_write, envir = env) else rm("write_json", envir = env)
  }, add = TRUE)

  expect_warning(
    write_json_files(df, countries, out_dir),
    regexp = "FAILED to create JSON for BAD_ONE"
  )

  expect_equal(written, c("DDI_OK_ONE_WB.json", "DDI_OK_TWO_WB.json"))
})

