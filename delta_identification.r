# Databricks notebook source
library(dplyr)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/delta_identification"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}
if (!exists("list_dta_files")) {
  source("helpers/filename_parsing.r")
}

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  # --- find harmonized data folders and dta files ---

  all_versions <- identify_all_versions(ROOT_DIR)

  # --- check already ingested and add new files to metadata table ---

  metadata <- tbl(sc, METADATA_TABLE) %>% collect()
  existing_paths <- metadata$dta_path
  new_files <- setdiff(all_versions$dta_path, existing_paths)
  print(paste("Files to be appended to the _ingestion_metadata table:", length(new_files)))

  if (length(new_files) > 0) {
    dataset_names <- basename(dirname(dirname(dirname(new_files))))
    new_filenames <- unique(dataset_names)
    print(new_filenames)

    meta_details <- all_versions %>% filter(dta_path %in% new_files)

    new_meta <- tibble(
      filename = dataset_names,
      table_name = make_table_name(new_files),
      dta_path = new_files,
      ingested = FALSE,
      published = FALSE,
      harmonization = NA_character_,
      household_level = NA,
      version_label = NA_character_,
      do_path = NA_character_,
      classification = NA_character_
    ) %>%
      left_join(
        meta_details %>%
          select(
            filename,
            country, year, quarter, survey,
            M_version, A_version
          ),
        by = c("filename" = "filename")
      )

    copy_to(sc, new_meta, "tmp_new_meta", overwrite = TRUE)

    cols <- colnames(new_meta)

    DBI::dbExecute(
      sc,
      paste0(
        "
        INSERT INTO ", METADATA_TABLE, " (", paste(cols, collapse = ", "), ")
        SELECT ", paste(paste0("t.", cols), collapse = ", "), "
        FROM tmp_new_meta t
        LEFT ANTI JOIN ", METADATA_TABLE, " m
        ON t.dta_path = m.dta_path
        "
      )
    )

    DBI::dbExecute(sc, "DROP TABLE IF EXISTS tmp_new_meta")
  }
}
