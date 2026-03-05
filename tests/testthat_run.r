# Databricks notebook source
install.packages("testthat")
library(testthat)

# COMMAND ----------

# MAGIC %run "../helpers/config"

# COMMAND ----------

if (!exists("is_databricks")) {
  library(testthat)
  results <- test_dir(file.path("tests", "testthat"))
  if (any(as.data.frame(results)$failed > 0)) {
    stop("Tests failed")
  }
  quit(save = "no", status = 0)
}

# COMMAND ----------

# MAGIC %run ./testthat/test_0_integration

# COMMAND ----------

# MAGIC %run ./testthat/test_2_integration

# COMMAND ----------

# MAGIC %run ./testthat/test_do_file_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_filename_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_github_links_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_harmonized_metadata_parsing

# COMMAND ----------

# MAGIC %run ./testthat/test_json_builder

# COMMAND ----------

# MAGIC %run ./testthat/test_json_pipeline

# COMMAND ----------

# MAGIC %run ./testthat/test_txt_pipeline

# COMMAND ----------

# MAGIC %run ./testthat/test_stacking_functions

# COMMAND ----------


