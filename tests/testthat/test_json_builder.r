# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(tibble)
  library(withr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/json_builder"

# COMMAND ----------

if (!exists("make_mdl_json")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source("helpers/json_builder.r")
}

# COMMAND ----------


make_minimal_row <- function(overrides = list()) {
  base <- list(
    filename = "USA_2020_LFS_V01_M_V01_A_GLD",
    survey_extended = "Labor Force Survey",
    survey_clean = "LFS",
    year = 2020,
    quarter = NA_character_,
    M_version = 1,
    A_version = 3,
    country = "USA",
    classification = "Official Use",
    version_label = NA_character_,
    gh_url = NA_character_,
    producers_name = NA_character_,
    household_level = FALSE,
    data_access_note = NA_character_,
    geog_coverage = NA_character_
  )

  as_tibble(modifyList(base, overrides))
}

# test mandatory fields
test_that("mandatory idno and title exist", {
  row <- make_minimal_row()
  countries <- tibble(code = "USA", name = "United States")

  js <- make_mdl_json(row, countries)

  expect_true(nzchar(js$idno))
  expect_true(nzchar(js$study_desc$title_statement$title))
})


# test fallbacks
test_that("quarter is included in title when present", {
  row <- make_minimal_row(list(quarter = "Q3"))
  countries <- tibble(code = "USA", name = "United States")

  js <- make_mdl_json(row, countries)

  expect_true(grepl(" Q3 ", js$study_desc$title_statement$title))
})


test_that("producer fallback uses NSO when producers_name is blank", {
  row <- make_minimal_row(list(producers_name = "   "))
  countries <- tibble(code = "USA", name = "United States")

  js <- make_mdl_json(row, countries)

  producer <- js$study_desc$production_statement$producers[[2]]$name
  expect_equal(producer, "National Statistical Offices of United States")
})


test_that("analysis_unit depends on household_level", {
  countries <- tibble(code = "USA", name = "United States")

  js1 <- make_mdl_json(make_minimal_row(list(household_level = FALSE)), countries)
  expect_equal(js1$study_desc$study_info$analysis_unit, "Individual")

  js2 <- make_mdl_json(make_minimal_row(list(household_level = TRUE)), countries)
  expect_equal(js2$study_desc$study_info$analysis_unit, "Individual and Household")
})


test_that("GH link is appended only when gh_url exists", {
  countries <- tibble(code = "USA", name = "United States")

  js_no <- make_mdl_json(make_minimal_row(), countries)
  expect_false(grepl("For more details see -->", js_no$study_desc$study_info$abstract))

  js_yes <- make_mdl_json(
    make_minimal_row(list(gh_url = "https://example.com")),
    countries
  )
  expect_true(grepl("https://example.com", js_yes$study_desc$study_info$abstract))
})




