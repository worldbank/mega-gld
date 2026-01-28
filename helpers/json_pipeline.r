# Databricks notebook source
library(dplyr)
library(readxl)
library(stringr)
library(purrr)
library(jsonlite)

# COMMAND ----------

# MAGIC %run "./json_builder"

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


compute_json_inputs <- function(metadata, survey, valid_pairs_df) {
  metadata %>%
    filter(published == FALSE) %>%
    left_join(survey, by = c("survey", "country")) %>%
    mutate(survey_clean = str_extract(survey, "^[^-]+")) %>%
    left_join(valid_pairs_df, by = c("country", "survey_clean"))
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
