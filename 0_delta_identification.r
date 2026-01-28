# Databricks notebook source
library(dplyr)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/filename_parsing"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}
if (!exists("list_dta_files")) {
  source("helpers/filename_parsing.r")
}

identify_latest_versions <- function(root_dir) {
  country_dirs <- list.dirs(root_dir, recursive = FALSE)
  dataset_dirs <- unlist(lapply(country_dirs, list.dirs, recursive = FALSE))
  version_dirs <- unlist(lapply(dataset_dirs, function(d) {
    list.dirs(d, recursive = FALSE)
  }))

  harmonized_paths <- file.path(version_dirs, "Data", "Harmonized")
  harmonized_paths <- harmonized_paths[dir.exists(harmonized_paths)]

  all_dta_files <- list_dta_files(harmonized_paths)
  print(paste("Dta files collected:", length(all_dta_files)))

  if (length(all_dta_files) == 0) {
    return(tibble())
  }

  parsed <- bind_rows(lapply(all_dta_files, parse_metadata_from_filename))
  latest_versions <- filter_latest_versions(parsed)
  print(paste(
    "Latest tables filtered:",
    nrow(latest_versions), "datasets (by country-year-survey),",
    "covering", n_distinct(latest_versions$country), "countries."
  ))

  latest_versions
}

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  # --- find harmonized data folders and dta files ---

  latest_versions <- identify_latest_versions(ROOT_DIR)
  latest_tables <- latest_versions$dta_path

  # --- check already ingested and add new files to metadata table ---

  metadata <- tbl(sc, METADATA_TABLE) %>% collect()
  already_ingested <- metadata$dta_path[metadata$ingested == TRUE]
  new_files <- setdiff(latest_tables, already_ingested)
  print(paste("New files to ingest:", length(new_files)))

  if (length(new_files) > 0) {
    dataset_names <- basename(dirname(dirname(dirname(new_files))))
    new_filenames <- unique(dataset_names)
    print(new_filenames)

    meta_details <- latest_versions %>% filter(dta_path %in% new_files)

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
