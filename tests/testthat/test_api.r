# Databricks notebook source
library(testthat)
library(haven)
library(zip)
library(withr)


# COMMAND ----------

# MAGIC %run "../../helpers/config"

# COMMAND ----------

# MAGIC %run "../../helpers/publication_pipeline"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

# COMMAND ----------

if (!exists("create_dataset")) {
  source("helpers/publication_pipeline.r")
}

# COMMAND ----------

config <- list(
  run_integration = TRUE,
  metadata_api_base = METADATA_API_BASE, 
  me_api_key = dbutils.secrets.get("GLDKEYVAULT","NADA_API_KEY"),
  project_id_for_upload = "25475"
)

if (!isTRUE(config$run_integration)) {
  message("Integration tests disabled in config. Skipping.")
} else {

  test_that("ME endpoint: create_dataset returns a non-empty id", {
    id <- create_dataset(
      json_data = list(
        title = paste0("integration_test_", as.integer(Sys.time())),
        test = TRUE
      ),
      ME_API_KEY = config$me_api_key
    )

    expect_false(is.na(id))
    expect_type(id, "integer")
    expect_gt(nchar(id), 0)
  })

  test_that("ME endpoint: upload_microdata_file returns file_id", {
    tmp <- tempfile(fileext = ".dta")
    haven::write_dta(data.frame(x = 1), tmp)

    file_id <- upload_microdata_file(
      project_id = config$project_id_for_upload,
      file_path  = tmp,
      ME_API_KEY  = config$me_api_key
    )

    expect_false(is.na(file_id))
    expect_type(file_id, "character")
    expect_gt(nchar(file_id), 0)
  })

  test_that("ME endpoint: create_resource returns id or TRUE", {
    txt <- tempfile(fileext = ".txt")
    writeLines("hello", txt)

    tmp <- tempfile(fileext = ".zip")
    zip::zip(tmp, files = txt)

    resource_body <- list(
      dctype      = "doc/tec",
      dcformat    = "application/zip",
      title       = "Integration Test Resource",
      author      = "Integration Test",
      description = "Created by Databricks integration test"
    )

    res <- create_resource(
      project_id     = config$project_id_for_upload,
      resource_body  = resource_body,
      file_path      = tmp,
      ME_API_KEY     = config$me_api_key
    )

    expect_true(isTRUE(res) || (is.character(res) && nchar(res) > 0))
  })

  message("All integration tests executed.")
}


