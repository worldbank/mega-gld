# Databricks notebook source
suppressPackageStartupMessages({
  library(dplyr)
  library(readxl)
  library(stringr)
  library(purrr)
  library(jsonlite)
})

# COMMAND ----------

# MAGIC %run "./json_builder"

# COMMAND ----------

if (!exists("make_mdl_json")) {
  source("helpers/json_builder.r")
}

# COMMAND ----------

fetch_countries_names <- function(sc, country_table = "prd_mega.indicator.country") {
  tbl(sc, country_table) %>%
    select(code = country_code, name = country_name) %>%
    collect()
}


fetch_survey_metadata <- function(root_dir, filename = "survey-metadata.xlsx") {
  path <- file.path(root_dir, filename)
  read_excel(path)
}


compute_json_inputs <- function(metadata, survey = NULL, valid_pairs_df = NULL) {
  out <- metadata %>%
    filter(
      published == FALSE,
      !is.na(classification), 
      trimws(classification) != ""
    )

  if (!is.null(survey)) {
    out <- out %>%
      left_join(survey, by = c("survey", "country")) %>%
      mutate(survey_clean = str_extract(survey, "^[^-]+"))
  } else {
    out <- out %>%
      mutate(survey_clean = str_extract(as.character(survey), "^[^-]+"))
  }

  if (!is.null(valid_pairs_df)) {
    out <- out %>%
      left_join(valid_pairs_df, by = c("country", "survey_clean"))
  }

  out
}


write_json_files <- function(df, countries_names, json_dir) {
  walk(seq_len(nrow(df)), function(i) {
    row <- df[i, ]

    tryCatch({
      json_obj <- make_mdl_json(row, countries_names)
      out_path <- file.path(json_dir, paste0("DDI_", row$filename, "_WB.json"))
      write_json(json_obj, out_path, pretty = TRUE, auto_unbox = TRUE)
      message("JSON created for ", row$filename)
    }, error = function(e) {
      warning("FAILED to create JSON for ", row$filename, ", ", conditionMessage(e))
    })
  })
}
