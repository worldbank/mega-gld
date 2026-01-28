# Databricks notebook source
library(dplyr)
library(sparklyr)


# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/gh_links"

# COMMAND ----------

# MAGIC %run "./helpers/json_pipeline"

# COMMAND ----------

if (is_databricks()) {
  sc <- spark_connect(method = "databricks")

  metadata <- tbl(sc, METADATA_TABLE) %>% collect()

  countries_names <- fetch_countries_names(sc)
  survey <- fetch_survey_metadata(ROOT_DIR)

  valid_pairs_df <- build_valid_pairs_df(
    gh_api_base  = GH_API_BASE,
    gh_path      = GH_PATH,
    gh_branch    = GH_BRANCH,
    gh_html_base = GH_HTML_BASE
  )

  merged_df <- compute_json_inputs(metadata, survey, valid_pairs_df)
  write_json_files(merged_df, countries_names, JSON_DIR)
}
