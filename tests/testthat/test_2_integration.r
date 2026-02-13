# Databricks notebook source
suppressPackageStartupMessages({
  library(testthat)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(withr)
})

# COMMAND ----------

# MAGIC %run "../../helpers/config"

# COMMAND ----------

# MAGIC %run "../../helpers/metadata_parsing"

# COMMAND ----------

if (!exists("find_txt_files")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "config.r"))
  source(file.path(repo_root, "helpers", "txt_parsing.r"))
  source(file.path(repo_root, "helpers", "do_file_parsing.r"))
  source(file.path(repo_root, "helpers", "metadata_parsing.r"))
}

# COMMAND ----------

create_test_metadata <- function(base_dir) {
  tibble::tibble(
    filename = c(
      "AAA_2020_SURV_V01_M_V01_A_GLD",
      "BBB_2019_SURV_V01_M_V01_A_GLD",
      "CCC_2018_SURV_V01_M_V01_A_GLD"
    ),
    dta_path = c(
      file.path(base_dir, "AAA/AAA_2020_SURV/AAA_2020_SURV_V01_M_V01_A_GLD/Data/Harmonized/AAA_2020_SURV_V01_M_V01_A_GLD_ALL.dta"),
      file.path(base_dir, "BBB/BBB_2019_SURV/BBB_2019_SURV_V01_M_V01_A_GLD/Data/Harmonized/BBB_2019_SURV_V01_M_V01_A_GLD_ALL.dta"),
      file.path(base_dir, "CCC/CCC_2018_SURV/CCC_2018_SURV_V01_M_V01_A_GLD/Data/Harmonized/CCC_2018_SURV_V01_M_V01_A_GLD_ALL.dta")
    ),
    published = c(FALSE, FALSE, TRUE),
    do_path = c(
      NA_character_,
      NA_character_,
      file.path(base_dir, "CCC/CCC_2018_SURV/CCC_2018_SURV_V01_M_V01_A_GLD/Programs/CCC_2018_SURV_V01_M_V01_A_GLD_ALL.do")
    ),
    version_label = c(NA_character_, NA_character_, "Existing label"),
    classification = c(NA_character_, "Official Use", NA_character_)
  )
}

# COMMAND ----------

test_that("metadata parsing pipeline updates do_path and version_label for unpublished rows", {
  tmp <- tempdir()
  base_dir <- file.path(tmp, "meta_parse")

  metadata <- create_test_metadata(base_dir)

  unpublished <- metadata %>%
    filter(published == FALSE, is.na(do_path))

  for (i in seq_len(nrow(unpublished))) {
    row <- unpublished[i, ]

    harmonized_dir <- dirname(row$dta_path)
    version_dir <- dirname(dirname(harmonized_dir))
    programs_dir <- file.path(version_dir, "Programs")

    dir.create(programs_dir, recursive = TRUE, showWarnings = FALSE)

    if (row$filename == "AAA_2020_SURV_V01_M_V01_A_GLD") {
      writeLines(
        c(
          "<_Version Control_>",
          "* 2021-01-01 - Initial version",
          "* 2022-02-02 - Added feature",
          "</_Version Control_>"
        ),
        file.path(programs_dir, "AAA_2020_SURV_V01_M_V01_A_GLD_ALL.do")
      )
    }

    if (row$filename == "BBB_2019_SURV_V01_M_V01_A_GLD") {
      writeLines(
        c(
          "<_Version Control_>",
          "* 2020-01-01 - Initial",
          "* 2021-01-01 - Description of changes",
          "</_Version Control_>"
        ),
        file.path(programs_dir, "BBB_2019_SURV_V01_M_V01_A_GLD_ALL.do")
      )
    }
  }

  updated <- suppressWarnings(
    compute_metadata_updates(metadata)
  )

  row_aaa <- updated %>% filter(filename == "AAA_2020_SURV_V01_M_V01_A_GLD")
  expect_true(grepl("AAA_2020_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_aaa$do_path))
  expect_equal(row_aaa$version_label[[1]], "Added feature")

  row_bbb <- updated %>% filter(filename == "BBB_2019_SURV_V01_M_V01_A_GLD")
  expect_true(grepl("BBB_2019_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_bbb$do_path))
  expect_equal(row_bbb$version_label[[1]], "Initial")

  row_ccc <- updated %>% filter(filename == "CCC_2018_SURV_V01_M_V01_A_GLD")
  expect_equal(row_ccc$version_label[[1]], "Existing label")
  expect_true(grepl("CCC_2018_SURV_V01_M_V01_A_GLD_ALL\\.do$", row_ccc$do_path))
})

# COMMAND ----------

test_that("metadata parsing pipeline updates classification for unpublished rows missing classification", {
  tmp <- tempdir()
  base_dir <- file.path(tmp, "meta_parse_class")

  metadata <- create_test_metadata(base_dir)

  unpublished_class <- metadata %>%
    filter(
      published == FALSE,
      is.na(classification) | trimws(classification) == ""
    )

  if (nrow(unpublished_class) == 0) {
    updated <- suppressWarnings(
      compute_metadata_updates(metadata)
    )
    expect_true(identical(metadata, updated))
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(unpublished_class))) {
    row <- unpublished_class[i, ]

    harmonized_dir <- dirname(row$dta_path)
    version_dir <- dirname(dirname(harmonized_dir))
    tech_dir <- file.path(version_dir, "Doc", "Technical")

    dir.create(tech_dir, recursive = TRUE, showWarnings = FALSE)

    txt_file <- file.path(tech_dir, paste0(row$filename, "_ReadMe.txt"))

    if (row$filename == "AAA_2020_SURV_V01_M_V01_A_GLD") {
      writeLines(
        c("Classification: OFFICIAL_USE", "Classification_code: OUO"),
        txt_file
      )
    } else if (row$filename == "BBB_2019_SURV_V01_M_V01_A_GLD") {
      writeLines(
        c("Classification: CONFIDENTIAL", "Classification_code: CONF"),
        txt_file
      )
    } else {
      writeLines(
        c("This file is for internal use only.", "No specific terms."),
        txt_file
      )
    }
  }

  updated <- suppressWarnings(
    compute_metadata_updates(metadata)
  )

  updated_missing <- updated %>%
    filter(
      published == FALSE,
      filename %in% unpublished_class$filename
    )

  expect_true(all(!is.na(updated_missing$classification)))
  expect_true(all(trimws(updated_missing$classification) != ""))

  if ("AAA_2020_SURV_V01_M_V01_A_GLD" %in% unpublished_class$filename) {
    aaa <- updated %>% filter(filename == "AAA_2020_SURV_V01_M_V01_A_GLD")
    expect_equal(aaa$classification[[1]], "Official Use")
  }

  if ("BBB_2019_SURV_V01_M_V01_A_GLD" %in% unpublished_class$filename) {
    bbb <- updated %>% filter(filename == "BBB_2019_SURV_V01_M_V01_A_GLD")
    expect_equal(bbb$classification[[1]], "Confidential")
  }
})
