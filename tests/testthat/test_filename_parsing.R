library(testthat)

source(file.path(dirname(dirname(getwd())), "helpers", "filename_parsing.R"))

# --- Tests for parse_metadata_from_filename ---

test_that("parse_metadata_from_filename extracts country code", {
  path <- "/root/USA/USA_2020_LFS_v01_M_v01_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$country, "USA")
})

test_that("parse_metadata_from_filename extracts year", {
  path <- "/root/BRA/BRA_2019_PNAD_v02_M_v01_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$year, "2019")
})

test_that("parse_metadata_from_filename extracts quarter when present", {
  path <- "/root/IND/IND_2021-Q3_PLFS_v01_M_v02_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$quarter, "Q3")
})

test_that("parse_metadata_from_filename returns NA for quarter when absent", {
  path <- "/root/USA/USA_2020_LFS_v01_M_v01_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_true(is.na(result$quarter))
})

test_that("parse_metadata_from_filename extracts survey name", {
  path <- "/root/MEX/MEX_2018_ENOE_v03_M_v02_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$survey, "ENOE")
})

test_that("parse_metadata_from_filename extracts M_version as integer", {
  path <- "/root/ARG/ARG_2022_EPH_v05_M_v03_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$M_version, 5L)
  expect_type(result$M_version, "integer")
})

test_that("parse_metadata_from_filename extracts A_version as integer", {
  path <- "/root/ARG/ARG_2022_EPH_v05_M_v03_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$A_version, 3L)
  expect_type(result$A_version, "integer")
})

test_that("parse_metadata_from_filename handles multi-word survey names with underscores", {
  path <- "/root/GBR/GBR_2020_LFS_ANNUAL_v02_M_v01_A/Data/Harmonized/file.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$survey, "LFS_ANNUAL")
})

test_that("parse_metadata_from_filename preserves full dta_path", {
  path <- "/root/USA/USA_2020_LFS_v01_M_v01_A/Data/Harmonized/survey.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$dta_path, path)
})

test_that("parse_metadata_from_filename extracts filename correctly", {
  path <- "/root/CAN/CAN_2021_LFS_v01_M_v02_A/Data/Harmonized/data.dta"
  result <- parse_metadata_from_filename(path)
  expect_equal(result$filename, "CAN_2021_LFS_v01_M_v02_A")
})

# --- Tests for filter_latest_versions ---

test_that("filter_latest_versions selects correct latest version for each country/year/survey", {
  parsed <- tibble(
    filename = c(
      # USA 2020 LFS: M_version takes priority (v02 wins over v01 despite lower A_version)
      "USA_2020_LFS_v01_M_v05_A",
      "USA_2020_LFS_v02_M_v01_A",
      # USA 2020 CPS: A_version breaks tie when M_version equal
      "USA_2020_CPS_v01_M_v01_A",
      "USA_2020_CPS_v01_M_v03_A",
      # USA 2021 LFS: different year, should be preserved separately
      "USA_2021_LFS_v01_M_v01_A",
      # BRA 2020 PNAD: different country, should be preserved separately
      "BRA_2020_PNAD_v01_M_v01_A"
    ),
    dta_path = c("/lfs_old", "/lfs_new", "/cps_old", "/cps_new", "/lfs_2021", "/bra_pnad"),
    country = c("USA", "USA", "USA", "USA", "USA", "BRA"),
    year = c("2020", "2020", "2020", "2020", "2021", "2020"),
    quarter = c(NA, NA, NA, NA, NA, NA),
    survey = c("LFS", "LFS", "CPS", "CPS", "LFS", "PNAD"),
    M_version = c(1L, 2L, 1L, 1L, 1L, 1L),
    A_version = c(5L, 1L, 1L, 3L, 1L, 1L)
  )

  result <- filter_latest_versions(parsed)

  # preserves distinct country/year/survey combinations

  expect_equal(nrow(result), 4)

  # M_version takes priority over A_version
  usa_2020_lfs <- result %>% filter(country == "USA", year == "2020", survey == "LFS")
  expect_equal(usa_2020_lfs$M_version, 2L)
  expect_equal(usa_2020_lfs$A_version, 1L)
  expect_equal(usa_2020_lfs$dta_path, "/lfs_new")

  # A_version breaks tie when M_version is equal
  usa_2020_cps <- result %>% filter(country == "USA", year == "2020", survey == "CPS")
  expect_equal(usa_2020_cps$M_version, 1L)
  expect_equal(usa_2020_cps$A_version, 3L)
  expect_equal(usa_2020_cps$dta_path, "/cps_new")

  # different years kept separate
  usa_2021_lfs <- result %>% filter(country == "USA", year == "2021", survey == "LFS")
  expect_equal(nrow(usa_2021_lfs), 1)
  expect_equal(usa_2021_lfs$dta_path, "/lfs_2021")

  # different countries kept separate
  bra_pnad <- result %>% filter(country == "BRA")
  expect_equal(nrow(bra_pnad), 1)
  expect_equal(bra_pnad$dta_path, "/bra_pnad")
})

# --- Tests for list_dta_files ---

test_that("list_dta_files finds .dta files across multiple paths and ignores other files", {
  tmp <- tempdir()
  dir1 <- file.path(tmp, "test_dir1")
  dir2 <- file.path(tmp, "test_dir2")
  dir3 <- file.path(tmp, "test_empty")
  dir.create(dir1, showWarnings = FALSE)
  dir.create(dir2, showWarnings = FALSE)
  dir.create(dir3, showWarnings = FALSE)

  file.create(file.path(dir1, "survey1.dta"))
  file.create(file.path(dir1, "survey2.dta"))
  file.create(file.path(dir1, "readme.txt"))
  file.create(file.path(dir2, "survey3.dta"))
  file.create(file.path(dir2, "data.csv"))

  result <- list_dta_files(c(dir1, dir2, dir3))

  expect_equal(length(result), 3)
  expect_true(all(grepl("\\.dta$", result)))
  expect_true(any(grepl("survey1\\.dta$", result)))
  expect_true(any(grepl("survey2\\.dta$", result)))
  expect_true(any(grepl("survey3\\.dta$", result)))

  unlink(c(dir1, dir2, dir3), recursive = TRUE)
})
