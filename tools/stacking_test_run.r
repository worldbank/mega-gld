# Databricks notebook source
# This notebook is not part of the main pipeline. It runs incremental_sync in test mode, i.e. on the sample created with tools/stacking_test_setup, and checks the resulting harmonized test tables against the source tables. Production tables are only read, never modified.

# COMMAND ----------

library(dplyr)
library(sparklyr)
library(DBI)

# COMMAND ----------

# MAGIC %run "../helpers/config"

# COMMAND ----------

# MAGIC %run "../helpers/stacking_schema"

# COMMAND ----------

sc <- spark_connect(method = "databricks")

CONFIDENTIAL_CLASS <- "Confidential"

METADATA_TEST <- paste0(METADATA_TABLE, "_test")
ALL_TEST      <- paste0(HARMONIZED_ALL, "_test")
OUO_TEST      <- paste0(HARMONIZED_OFFICIAL, "_test")

# COMMAND ----------

# Run incremental_sync on the sample
dbutils.notebook.run("../incremental_sync", 7200, list(test_mode = "true"))

# COMMAND ----------

# Number of rows, and of non-null values in each of the given columns
profile_table <- function(table, cols, where = "1 = 1") {
  counts <- paste0(sprintf(", COUNT(`%s`) AS `%s`", cols, cols), collapse = "")
  DBI::dbGetQuery(sc, sprintf(
    "SELECT COUNT(*) AS n_rows%s FROM %s WHERE %s", counts, table, where
  ))
}

# Check that a harmonized table contains exactly the given surveys, each with the
# same number of rows and of non-null dynamic column values as its source table
check_harmonized_table <- function(harmonized_table, surveys) {
  expected_cols <- names(get_gld_schema())
  harmonized_cols <- colnames(tbl(sc, harmonized_table))
  failures <- character()
  expected_total <- 0

  for (i in seq_len(nrow(surveys))) {
    s <- surveys[i, ]
    src_table <- paste0(TARGET_SCHEMA, ".", s$table_name)
    dynamic_cols <- setdiff(
      Filter(is_dynamic_column, colnames(tbl(sc, src_table))),
      expected_cols
    )

    expected <- profile_table(src_table, dynamic_cols)
    expected_total <- expected_total + expected$n_rows

    missing_cols <- setdiff(dynamic_cols, harmonized_cols)
    if (length(missing_cols) > 0) {
      failures <- c(failures, sprintf(
        "%s: dynamic column(s) missing: %s",
        s$table_name, paste(missing_cols, collapse = ", ")
      ))
      next
    }

    actual <- profile_table(harmonized_table, dynamic_cols, sprintf(
      "countrycode = '%s' AND year = %d AND survname = '%s' AND quarter = '%s'",
      s$country, as.integer(s$year), s$survey, s$quarter
    ))

    mismatches <- names(expected)[as.numeric(unlist(expected)) != as.numeric(unlist(actual))]
    if (length(mismatches) > 0) {
      failures <- c(failures, sprintf(
        "%s: %s (source %s, harmonized %s)",
        s$table_name, mismatches, unlist(expected[mismatches]), unlist(actual[mismatches])
      ))
    } else {
      message(sprintf("✓ %s: %d row(s), %d dynamic column(s) match the source table",
                      s$table_name, expected$n_rows, length(dynamic_cols)))
    }
  }

  actual_total <- profile_table(harmonized_table, character())$n_rows
  if (actual_total != expected_total) {
    failures <- c(failures, sprintf(
      "total rows: expected %d from the %d survey(s) of the sample, found %d",
      expected_total, nrow(surveys), actual_total
    ))
  }

  if (length(failures) > 0) {
    stop(
      sprintf("%s does not match the source tables:\n - %s",
              harmonized_table, paste(failures, collapse = "\n - ")),
      call. = FALSE
    )
  }
  message(sprintf("✓ %s matches the source tables (%d survey(s), %d row(s))",
                  harmonized_table, nrow(surveys), actual_total))
}

# COMMAND ----------

surveys <- tbl(sc, METADATA_TEST) %>%
  filter(stacking == 1) %>%
  select(table_name, classification, country, year, survey, quarter) %>%
  distinct() %>%
  collect()

# COMMAND ----------

check_harmonized_table(ALL_TEST, surveys)

# COMMAND ----------

check_harmonized_table(OUO_TEST, surveys %>% filter(classification != CONFIDENTIAL_CLASS))
