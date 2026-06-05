# Databricks notebook source
# This notebook is not part of the main pipeline. It can be used to set the "published" flag in _ingestion_metadata to FALSE. As a result, the dataset will be re-published next time the main pipeline runs. 

# COMMAND ----------

library(dplyr)
library(sparklyr)
library(DBI)

# COMMAND ----------

# MAGIC %run "../helpers/config"

# COMMAND ----------

# Please enter the ids of the files you wish to republish, as
# ids <- c("id_1", "id_2")

# If you need to republish multiple tables, you may query the _ingestion_metadata table and assign the filename column to ids

ids <- character(0)

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

DBI::dbExecute(sc, paste0("
  UPDATE ", METADATA_TABLE, "
  SET published = FALSE
  WHERE filename IN (",
  paste(paste0("'", ids, "'"), collapse = ", "),
  ")"
))
