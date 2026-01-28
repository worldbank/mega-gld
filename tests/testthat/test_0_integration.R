library(testthat)
library(dplyr)
library(withr)

repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
withr::local_dir(repo_root)
source(file.path(repo_root, "0_delta_identification.r"))

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
      dataset = "IND_2021-Q1_PLFS",
      version = "IND_2021-Q1_PLFS_V01_M_V01_A_GLD",
      dta = "IND_2021-Q1_PLFS_V01_M_V01_A_GLD_ALL.dta"
    ),
    list(
      country = "IND",
      dataset = "IND_2022-Q3_PLFS",
      version = "IND_2022-Q3_PLFS_V01_M_V01_A_GLD",
      dta = "IND_2022-Q3_PLFS_V01_M_V01_A_GLD_ALL.dta"
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

  expect_equal(nrow(latest_versions), 5)

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

  # Quarter extraction
  q1 <- latest_versions %>% filter(quarter == "Q1")
  q3 <- latest_versions %>% filter(quarter == "Q3")
  expect_equal(nrow(q1), 1)
  expect_equal(nrow(q3), 1)
  expect_equal(q1$country, "IND")
  expect_equal(q1$year, "2021")
  expect_equal(q3$year, "2022")

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
