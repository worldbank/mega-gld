# Databricks notebook source
# This notebook is not part of the main pipeline. It can be used to clear the "classification" flag in _ingestion_metadata if the data classification needs to be recomputed
# It also resets the stacked table versions so the next incremental sync drops the rows from both harmonized tables and restacks them according to the new classification
# Run metadata parsing (to recompute the classification) BEFORE the next incremental sync

# COMMAND ----------

# IMPORTANT: To republish microdata files after offline edits, verify the new files have synced to the volume BEFORE running this script 

# COMMAND ----------

library(dplyr)
library(sparklyr)
library(DBI)

# COMMAND ----------

# MAGIC %run "../helpers/config"

# COMMAND ----------

# Please enter the ids of the files you wish to republish, as
#ids <- c("id_1", "id_2")

# If you need to republish multiple tables, you may query the _ingestion_metadata table and assign the filename column to ids

ids <- c("ROU_2009_AMIGO_V01_M_V01_A_GLD", "ROU_2020_AMIGO_V01_M_V02_A_GLD", "ROU_2021_AMIGO_V01_M_V02_A_GLD", "ROU_2022_AMIGO_V01_M_V02_A_GLD", "ROU_2023_AMIGO_V01_M_V02_A_GLD")


# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

if (length(ids) == 0) stop("No ids provided")
DBI::dbExecute(sc, paste0("
  UPDATE ", METADATA_TABLE, "
  SET classification = NULL,
      published = FALSE,
      stacked_all_table_version = NULL,
      stacked_ouo_table_version = NULL
  WHERE filename IN (",
  paste(paste0("'", ids, "'"), collapse = ", "),
  ")"
))
