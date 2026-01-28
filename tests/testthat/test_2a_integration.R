library(testthat)
library(dplyr)
library(purrr)
library(readr)
library(withr)

repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
withr::local_dir(repo_root)
source(file.path(repo_root, "2a_metadata_parsing.r"))

test_that("metadata parsing pipeline updates do_path and version_label for unpublished rows", {

  fixture_path <- normalizePath(
    file.path(repo_root, "tests", "fixtures", "sample_metadata_2a_input.csv"),
    mustWork = TRUE
  )

  metadata <- readr::read_csv(fixture_path, show_col_types = FALSE)

  tmp <- tempdir()
  base_dir <- file.path(tmp, "meta_parse")

  metadata <- metadata %>%
    mutate(
      dta_path = gsub("^BASE", base_dir, dta_path),
      do_path = ifelse(is.na(do_path), NA_character_, gsub("^BASE", base_dir, do_path))
    )

  unpublished <- metadata %>%
    filter(published == FALSE, is.na(do_path))

  for (i in seq_len(nrow(unpublished))) {
    row <- unpublished[i, ]
    harmonized_dir <- dirname(row$dta_path)
    version_dir <- dirname(dirname(harmonized_dir))
    programs_dir <- file.path(version_dir, "Programs")

    dir.create(programs_dir, recursive = TRUE, showWarnings = FALSE)

    if (row$filename == "AAA_2020_SURV_V01_M_V01_A_GLD") {
      do_file <- file.path(programs_dir, "AAA_2020_SURV_V01_M_V01_A_GLD_ALL.do")
      writeLines(c(
        "<_Version Control_>",
        "* 2021-01-01 - Initial version",
        "* 2022-02-02 - Added feature",
        "</_Version Control_>"
      ), do_file)
    }

    if (row$filename == "BBB_2019_SURV_V01_M_V01_A_GLD") {
      do_file <- file.path(programs_dir, "BBB_2019_SURV_V01_M_V01_A_GLD_ALL.do")
      writeLines(c(
        "<_Version Control_>",
        "* 2020-01-01 - Initial",
        "* 2021-01-01 - Description of changes",
        "</_Version Control_>"
      ), do_file)
    }
  }

  metadata <- compute_metadata_updates(metadata)

  row_aaa <- metadata %>% filter(filename == "AAA_2020_SURV_V01_M_V01_A_GLD")
  expect_true(grepl("AAA_2020_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_aaa$do_path))
  expect_equal(row_aaa$version_label[[1]], "Added feature")

  row_bbb <- metadata %>% filter(filename == "BBB_2019_SURV_V01_M_V01_A_GLD")
  expect_true(grepl("BBB_2019_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_bbb$do_path))
  expect_equal(row_bbb$version_label[[1]], "Initial")

  row_ccc <- metadata %>% filter(filename == "CCC_2018_SURV_V01_M_V01_A_GLD")
  expect_equal(row_ccc$version_label[[1]], "Existing label")
  expect_true(grepl("CCC_2018_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_ccc$do_path))
})
