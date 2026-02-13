# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(withr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/gh_links_parsing"
# MAGIC

# COMMAND ----------

if (!exists("gh_list_dirs")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "gh_links_parsing.r"))
}

# COMMAND ----------

test_that("build_valid_pairs_df returns empty tibble when API fails", {

  old <- gh_list_dirs
  on.exit(gh_list_dirs <<- old, add = TRUE)

  gh_list_dirs <<- function(url) NULL

  out <- build_valid_pairs_df("X", "Y", "main", "H")

  expect_equal(nrow(out), 0)
  expect_equal(names(out), c("country", "survey_clean", "gh_url"))
})



test_that("build_valid_pairs_df builds gh_url for valid country dirs", {

  fake_root <- list(
    list(type = "dir", name = "USA"),
    list(type = "file", name = "README.md")
  )

  fake_country <- list(
    list(type = "dir", name = "LFS")
  )

  old <- gh_list_dirs
  on.exit(gh_list_dirs <<- old, add = TRUE)

  gh_list_dirs <<- function(url) {
    if (!grepl("USA", url)) return(fake_root)
    fake_country
  }

  out <- build_valid_pairs_df("X", "Y", "main", "H")

  expect_equal(out$country, "USA")
  expect_equal(out$survey_clean, "LFS")
  expect_true(grepl("^H/", out$gh_url))
})
