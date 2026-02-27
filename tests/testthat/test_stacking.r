# Databricks notebook source
suppressPackageStartupMessages({  
  library(testthat)
  library(dplyr)
  library(stringr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/stacking_flag_compute"

# COMMAND ----------

if (!exists("compute_stacking")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "stacking_flag_compute.r"))
}

# COMMAND ----------

test_that("compute_stacking works correctly on provided fixture", {

  df <- tibble(
    filename = c(
      "COL_2019_GEIH_V01_M_V03_A_GLD",
      "COL_2019_GEIH_V01_M_V04_A_GLD",
      "THA_2019_LFS-Q1_V01_M_V03_A_GLD",
      "THA_2019_LFS-Q2_V01_M_V03_A_GLD",
      "THA_2019_LFS-Q2_V02_M_V03_A_GLD",
      "MEX_2005_ENOE_V02_M_V02_A_GLD",
      "MEX_2005-2023_ENOE_V01_M_V01_A_GLD"
    ),
    table_name = c(
      "col_2019_geih",
      "col_2019_geih",
      "tha_2019_lfs_q1",
      "tha_2019_lfs_q2",
      "tha_2019_lfs_q2",
      "mex_2005_enoe",
      "mex_2023_enoe_panel"
    ),
    harmonization = c(
      "GLD", "GLD", "GLD", "GLD", "GLD", "GLD", "GLD"
    ),
    country = c(
      "COL", "COL", "THA", "THA", "THA", "MEX", "MEX"
    ),
    year = c(
      2019, 2019, 2019, 2019, 2019, 2005, 2005
    ),
    quarter = c(
      NA, NA, "Q1", "Q2", "Q2", NA, NA
    ),
    classification = c(
      "Official Use",
      NA,
      "Official Use",
      "Official Use",
      "Official Use",
      "Official Use",
      "Official Use"
    ),
    A_version = c(
      3, 4, 3, 3, 3, 2, 1
    ),
    M_version = c(
      1, 1, 1, 1, 2, 2, 1
    )
  )

  res <- compute_stacking(df)

  # ---- exact expected stacking ----
  expect_equal(
    res$stacking,
    c(
      1L,  # COL annual V03 (fallback from unclassified V04)
      0L,
      1L,  # THA Q1
      0L,  # THA Q2 old
      1L,  # THA Q2 latest
      1L,  # MEX annual (non-panel)
      0L   # MEX panel
    )
  )

  # ---- invariants ----

  # panel rows never stack
  expect_true(all(
    res$stacking[str_detect(res$table_name, "panel")] == 0L
  ))

  # at most one annual row per country-year stacks
  annual <- res %>%
    filter(is.na(quarter), !str_detect(table_name, "panel")) %>%
    group_by(country, year) %>%
    summarise(n = sum(stacking), .groups = "drop")

  expect_true(all(annual$n <= 1))

  # at most one quarterly row per country-year-quarter stacks
  quarterly <- res %>%
    filter(!is.na(quarter)) %>%
    group_by(country, year, quarter) %>%
    summarise(n = sum(stacking), .groups = "drop")

  expect_true(all(quarterly$n <= 1))

  # stacked rows must have classification
  expect_true(all(
    res$stacking == 0L | !is.na(res$classification)
  ))
})
