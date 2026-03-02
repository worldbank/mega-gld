# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(withr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/config"

# COMMAND ----------

# MAGIC %run "../../helpers/delta_identification"

# COMMAND ----------

if (!exists("list_dta_files")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "config.r"))
  source(file.path(repo_root, "helpers", "delta_identification.r"))
}

# COMMAND ----------

test_that("delta identification pipeline finds dta files and filters to latest versions", {
  tmp <- tempdir()
  root_dir <- file.path(tmp, "gld_root")
  unlink(root_dir, recursive = TRUE)

  datasets <- list(
    list(
      country = "USA",
      dataset = "USA_2020_LFS",
      version = "USA_2020_LFS_V01_M_V01_A_GLD",
      dta = "USA_2020_LFS_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "USA",
      dataset = "USA_2020_LFS",
      version = "USA_2020_LFS_V02_M_V01_A_GLD",
      dta = "USA_2020_LFS_V02_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "USA",
      dataset = "USA_2021_CPS",
      version = "USA_2021_CPS_V01_M_V01_A_GLD",
      dta = "USA_2021_CPS_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "BRA",
      dataset = "BRA_2019_PNAD",
      version = "BRA_2019_PNAD_V01_M_V01_A_GLD",
      dta = "BRA_2019_PNAD_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "BRA",
      dataset = "BRA_2019_PNAD",
      version = "BRA_2019_PNAD_V01_M_V02_A_GLD",
      dta = "BRA_2019_PNAD_V01_M_V02_A_GLD_ALL.dta"
    ),
    list(
      country = "IND",
      dataset = "IND_2021_PLFS-Q1",
      version = "IND_2021_PLFS-Q1_V01_M_V01_A_GLD",
      dta = "IND_2021_PLFS-Q1_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "IND",
      dataset = "IND_2021_PLFS-Q2",
      version = "IND_2021_PLFS-Q2_V01_M_V01_A_GLD",
      dta     = "IND_2021_PLFS-Q2_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "IND",
      dataset = "IND_2022_PLFS-Q3",
      version = "IND_2022_PLFS-Q3_V01_M_V01_A_GLD",
      dta = "IND_2022_PLFS-Q3_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "IND",
      dataset = "IND_2022_PLFS-Q3",
      version = "IND_2022_PLFS-Q3_V01_M_V02_A_GLD",
      dta     = "IND_2022_PLFS-Q3_V01_M_V02_A_GLD_ALL.dta"
    )
  )

  for (ds in datasets) {
    harmonized_dir <- file.path(
      root_dir, ds$country, ds$dataset, ds$version, "Data", "Harmonized"
    )
    dir.create(harmonized_dir, recursive = TRUE, showWarnings = FALSE)
    file.create(file.path(harmonized_dir, ds$dta))
  }

  latest_versions <- identify_latest_versions(root_dir)

  expect_equal(nrow(latest_versions), 6)

  # M_version takes priority
  usa_lfs <- latest_versions %>% filter(country == "USA", survey == "LFS")
  expect_equal(nrow(usa_lfs), 1)
  expect_equal(usa_lfs$M_version, 2L)
  expect_true(grepl("V02_M_V01_A", usa_lfs$dta_path))

  # A_version breaks ties
  bra_pnad <- latest_versions %>% filter(country == "BRA", survey == "PNAD")
  expect_equal(nrow(bra_pnad), 1)
  expect_equal(bra_pnad$A_version, 2L)
  expect_true(grepl("V01_M_V02_A", bra_pnad$dta_path))

  usa_cps <- latest_versions %>% filter(country == "USA", survey == "CPS")
  expect_equal(nrow(usa_cps), 1)

  # Quarter extraction + quarter-aware grouping keeps both Q1 and Q2 for IND 2021 PLFS
  ind_2021_plfs <- latest_versions %>% filter(country == "IND", year == "2021", survey == "PLFS")
  expect_equal(nrow(ind_2021_plfs), 2)
  expect_equal(sort(ind_2021_plfs$quarter), c("Q1", "Q2"))

  q1 <- latest_versions %>% filter(country == "IND", year == "2021", survey == "PLFS", quarter == "Q1")
  q2 <- latest_versions %>% filter(country == "IND", year == "2021", survey == "PLFS", quarter == "Q2")
  expect_equal(nrow(q1), 1)
  expect_equal(nrow(q2), 1)

  # Within-quarter version selection works for quarterly data too (IND 2022 Q3 picks A_version = 2)
  ind_2022_q3 <- latest_versions %>% filter(country == "IND", year == "2022", survey == "PLFS", quarter == "Q3")
  expect_equal(nrow(ind_2022_q3), 1)
  expect_equal(ind_2022_q3$A_version, 2L)
  expect_true(grepl("IND_2022-Q3_PLFS_V01_M_V02_A", ind_2022_q3$dta_path))

  unlink(root_dir, recursive = TRUE)
})

test_that("delta identification returns empty results for empty directory", {
  tmp <- tempdir()
  root_dir <- file.path(tmp, "gld_root_empty")
  unlink(root_dir, recursive = TRUE)
  dir.create(root_dir, recursive = TRUE)

  latest_versions <- identify_latest_versions(root_dir)

  expect_equal(nrow(latest_versions), 0)

  unlink(root_dir, recursive = TRUE)
})
