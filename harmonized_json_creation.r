# Databricks notebook source
library(dplyr)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/json_pipeline"

# COMMAND ----------

# MAGIC %run "./helpers/harmonized_metadata_parsing"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

if (!exists("fetch_countries_names")) {
  source("helpers/json_pipeline.r")
}

if (!exists("get_unique_vec")) {
  source("helpers/harmonized_metadata_parsing.r")
}

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  metadata <- tbl(sc, METADATA_TABLE)
  
  tbl_all <- tbl(sc,  paste0(TARGET_SCHEMA,".gld_harmonized_all"))
  tbl_ouo <- tbl(sc, paste0(TARGET_SCHEMA,".gld_harmonized_ouo"))

  metadata_harmonized <- build_harmonized_metadata(
    sc = sc,
    tbl_metadata = metadata,
    tbl_all = tbl_all,
    tbl_ouo = tbl_ouo
  )

  countries_names <- fetch_countries_names(sc)

  harmonized <- compute_json_inputs(metadata_harmonized)
  write_json_files(harmonized, countries_names, JSON_DIR)
}
