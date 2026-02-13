# Databricks notebook source
suppressPackageStartupMessages({  
  library(testthat)
  library(fs)
  library(readr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/txt_parsing"

# COMMAND ----------

if (!exists("find_txt_files")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "txt_parsing.r"))
}

# COMMAND ----------

make_harmonized_path <- function(base_dir,
                                 country = "AFG",
                                 dataset = "AFG_2007_NRVA",
                                 version = "AFG_2007_NRVA_V01_M_V01_A_GLD") {
  fs::path(base_dir, country, dataset, version, "Data", "Harmonized")
}

test_that("find_txt_files returns ReadMe.txt from Doc/Technical when it exists", {
  tmp <- tempdir()
  base_dir <- fs::path(tmp, "txt_case_tech")

  harmonized_path <- make_harmonized_path(base_dir)
  fs::dir_create(harmonized_path, recurse = TRUE)

  version_folder <- fs::path_dir(fs::path_dir(fs::path_dir(harmonized_path)))
  fs::dir_create(fs::path(version_folder, "Doc", "Technical"), recurse = TRUE)

  f <- fs::path(version_folder, "Doc", "Technical", "Where is this data from - ReadMe.txt")
  writeLines("Classification: OFFICIAL USE", f)

  out <- find_txt_files(harmonized_path, "AFG_2007_NRVA_V01_M_V01_A_GLD")
  expect_equal(out, as.character(f))

  unlink(base_dir, recursive = TRUE)
})

test_that("find_txt_files falls back to Doc when Doc/Technical is missing", {
  tmp <- tempdir()
  base_dir <- fs::path(tmp, "txt_case_doc")

  harmonized_path <- make_harmonized_path(
    base_dir,
    country = "USA",
    dataset = "USA_2020_LFS",
    version = "USA_2020_LFS_V01_M_V01_A_GLD"
  )
  fs::dir_create(harmonized_path, recurse = TRUE)

  version_folder <- fs::path_dir(fs::path_dir(fs::path_dir(harmonized_path)))
  fs::dir_create(fs::path(version_folder, "Doc"), recurse = TRUE)

  f <- fs::path(version_folder, "Doc", "Some prefix - ReadMe.txt")
  writeLines("Classification: CONFIDENTIAL", f)

  out <- find_txt_files(harmonized_path, "USA_2020_LFS_V01_M_V01_A_GLD")
  expect_equal(out, as.character(f))

  unlink(base_dir, recursive = TRUE)
})

test_that("find_txt_files warns and returns empty string when no Doc folders exist", {
  tmp <- tempdir()
  base_dir <- fs::path(tmp, "txt_case_none")

  harmonized_path <- make_harmonized_path(
    base_dir,
    country = "XKX",
    dataset = "XKX_2019_LFS",
    version = "XKX_2019_LFS_V01_M_V02_A_GLD"
  )
  fs::dir_create(harmonized_path, recurse = TRUE)

  expect_warning(
    out <- find_txt_files(harmonized_path, "XKX_2019_LFS_V01_M_V02_A_GLD"),
    "No Doc or Technical directory found"
  )
  expect_equal(out, "")

  unlink(base_dir, recursive = TRUE)
})

test_that("find_txt_files warns and returns empty string when 0 or >1 ReadMe.txt files exist", {
  tmp <- tempdir()
  base_dir <- fs::path(tmp, "txt_case_multi")

  harmonized_path <- make_harmonized_path(
    base_dir,
    country = "COL",
    dataset = "COL_2021_GEIH",
    version = "COL_2021_GEIH_V01_M_V05_A_GLD"
  )
  fs::dir_create(harmonized_path, recurse = TRUE)

  version_folder <- fs::path_dir(fs::path_dir(fs::path_dir(harmonized_path)))
  fs::dir_create(fs::path(version_folder, "Doc"), recurse = TRUE)

  # 0 matching files
  expect_warning(
    out0 <- find_txt_files(harmonized_path, "COL_2021_GEIH_V01_M_V05_A_GLD"),
    "No txt file found"
  )
  expect_equal(out0, "")

  # >1 matching files
  f1 <- fs::path(version_folder, "Doc", "One - ReadMe.txt")
  f2 <- fs::path(version_folder, "Doc", "Two - ReadMe.txt")
  writeLines("x", f1)
  writeLines("y", f2)

  expect_warning(
    out2 <- find_txt_files(harmonized_path, "COL_2021_GEIH_V01_M_V05_A_GLD"),
    "No txt file found"
  )
  expect_equal(out2, "")

  unlink(base_dir, recursive = TRUE)
})

test_that("detect_classification returns empty and warns when txt path is missing", {
  expect_warning(
    out <- detect_classification(NA_character_, "USA_2020_LFS_V01_M_V01_A_GLD"),
    "txt file missing"
  )
  expect_equal(out, "")

  expect_warning(
    out2 <- detect_classification("not_a_real_file.txt", "USA_2020_LFS_V01_M_V01_A_GLD"),
    "txt file missing"
  )
  expect_equal(out2, "")
})

test_that("detect_classification detects explicit Classification tag", {
  tmp <- tempfile(fileext = ".txt")
  writeLines("Header\nClassification: OFFICIAL_USE\nFooter", tmp)
  expect_equal(detect_classification(tmp, "USA_2020_LFS_V01_M_V01_A_GLD"), "Official Use")

  writeLines("Classification: CONFIDENTIAL", tmp)
  expect_equal(detect_classification(tmp, "USA_2020_LFS_V01_M_V01_A_GLD"), "Confidential")
})

test_that("detect_classification falls back to keyword heuristics", {
  tmp <- tempfile(fileext = ".txt")

  writeLines("This dataset is confidential and needs to approve before sharing.", tmp)
  expect_equal(detect_classification(tmp, "USA_2020_LFS_V01_M_V01_A_GLD"), "Confidential")

  writeLines("This is publicly available and freely available for use.", tmp)
  expect_equal(detect_classification(tmp, "USA_2020_LFS_V01_M_V01_A_GLD"), "Official Use")
})

test_that("detect_classification warns and returns empty when nothing matches", {
  tmp <- tempfile(fileext = ".txt")
  writeLines("Nothing useful here.", tmp)

  expect_warning(
    out <- detect_classification(tmp, "USA_2020_LFS_V01_M_V01_A_GLD"),
    "could not detect data classification"
  )
  expect_equal(out, "")
})

