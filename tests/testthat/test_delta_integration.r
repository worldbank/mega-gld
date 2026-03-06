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

test_that("delta identification pipeline finds dta files and parses all versions", {
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

  all_versions <- identify_all_versions(root_dir)

  # now all files should be returned, not just the latest ones
  expect_equal(nrow(all_versions), 9)

  # check that repeated versions are all retained
  usa_lfs <- all_versions %>% filter(country == "USA", year == "2020", survey == "LFS")
  expect_equal(nrow(usa_lfs), 2)
  expect_equal(sort(usa_lfs$M_version), c(1L, 2L))

  bra_pnad <- all_versions %>% filter(country == "BRA", year == "2019", survey == "PNAD")
  expect_equal(nrow(bra_pnad), 2)
  expect_equal(sort(bra_pnad$A_version), c(1L, 2L))

  usa_cps <- all_versions %>% filter(country == "USA", year == "2021", survey == "CPS")
  expect_equal(nrow(usa_cps), 1)
  expect_equal(usa_cps$M_version, 1L)
  expect_equal(usa_cps$A_version, 1L)

  # quarterly parsing still keeps Q1 and Q2 separate
  ind_2021_plfs <- all_versions %>%
    filter(country == "IND", year == "2021", survey == "PLFS")
  expect_equal(nrow(ind_2021_plfs), 2)
  expect_equal(sort(ind_2021_plfs$quarter), c("Q1", "Q2"))

  q1 <- all_versions %>% filter(country == "IND", year == "2021", survey == "PLFS", quarter == "Q1")
  q2 <- all_versions %>% filter(country == "IND", year == "2021", survey == "PLFS", quarter == "Q2")
  expect_equal(nrow(q1), 1)
  expect_equal(nrow(q2), 1)

  # both versions for IND 2022 Q3 should now be present
  ind_2022_q3 <- all_versions %>%
    filter(country == "IND", year == "2022", survey == "PLFS", quarter == "Q3")
  expect_equal(nrow(ind_2022_q3), 2)
  expect_equal(sort(ind_2022_q3$A_version), c(1L, 2L))
  expect_true(any(grepl("IND_2022_PLFS-Q3_V01_M_V01_A", ind_2022_q3$dta_path)))
  expect_true(any(grepl("IND_2022_PLFS-Q3_V01_M_V02_A", ind_2022_q3$dta_path)))

  unlink(root_dir, recursive = TRUE)
})

test_that("delta identification returns empty results for empty directory", {
  tmp <- tempdir()
  root_dir <- file.path(tmp, "gld_root_empty")
  unlink(root_dir, recursive = TRUE)
  dir.create(root_dir, recursive = TRUE)

  all_versions <- identify_all_versions(root_dir)

  expect_equal(nrow(all_versions), 0)

  unlink(root_dir, recursive = TRUE)
})


test_that("files already present in metadata are not selected as new", {
  all_versions <- tibble(
    filename = c(
      "USA_2020_LFS_V01_M_V01_A_GLD",
      "USA_2020_LFS_V02_M_V01_A_GLD",
      "BRA_2019_PNAD_V01_M_V01_A_GLD"
    ),
    dta_path = c(
      "/root/USA/USA_2020_LFS/USA_2020_LFS_V01_M_V01_A_GLD/Data/Harmonized/file1.dta",
      "/root/USA/USA_2020_LFS/USA_2020_LFS_V02_M_V01_A_GLD/Data/Harmonized/file2.dta",
      "/root/BRA/BRA_2019_PNAD/BRA_2019_PNAD_V01_M_V01_A_GLD/Data/Harmonized/file3.dta"
    )
  )

  metadata <- tibble(
    dta_path = c(
      "/root/USA/USA_2020_LFS/USA_2020_LFS_V01_M_V01_A_GLD/Data/Harmonized/file1.dta"
    )
  )

  existing_paths <- metadata$dta_path
  new_files <- setdiff(all_versions$dta_path, existing_paths)

  expect_equal(length(new_files), 2)
  expect_false("/root/USA/USA_2020_LFS/USA_2020_LFS_V01_M_V01_A_GLD/Data/Harmonized/file1.dta" %in% new_files)
  expect_true("/root/USA/USA_2020_LFS/USA_2020_LFS_V02_M_V01_A_GLD/Data/Harmonized/file2.dta" %in% new_files)
  expect_true("/root/BRA/BRA_2019_PNAD/BRA_2019_PNAD_V01_M_V01_A_GLD/Data/Harmonized/file3.dta" %in% new_files)
})
