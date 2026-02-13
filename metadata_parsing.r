# Databricks notebook source
library(purrr)
library(dplyr)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/do_file_parsing"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}
if (!exists("find_do_files")) {
  source("helpers/do_file_parsing.r")
}

compute_metadata_updates <- function(metadata) {
  unpublished <- metadata %>%
    filter(
      published == FALSE,
      is.na(do_path)
    )

  print(paste("Found", nrow(unpublished), "unpublished records missing do_path"))

  if (nrow(unpublished) == 0) {
    return(metadata)
  }

  print("Finding do files...")
  unpublished <- unpublished %>%
    mutate(do_path = map2_chr(dta_path, filename, find_do_files))

  print("Parsing do files...")
  parsed_rows <- lapply(seq_len(nrow(unpublished)), function(i) {
    row <- unpublished[i, ]
    do_file <- row$do_path[[1]]

    if (is.character(do_file) && file.exists(do_file)) {
      info <- parse_do_file(do_file)
    } else {
      info <- list()
    }

    info$filename <- row$filename
    info
  })

  parsed_df <- bind_rows(parsed_rows)
  print(paste("Parsed", nrow(parsed_df), "do files"))

  # --- compute version ---
  v_cols <- grep("^V\\d{2}$", names(parsed_df), value = TRUE)

  parsed_df$latest_v_text <- apply(parsed_df[v_cols], 1, function(x) {
    select_latest_version(as.list(x), v_cols)
  })

  parsed_df$version_label <- vapply(parsed_df$latest_v_text, make_version_label, character(1))

  # --- add to metadata table ---
  metadata %>%
    mutate(
      version_label = coalesce(
        parsed_df$version_label[match(filename, parsed_df$filename)],
        version_label
      ),
      do_path = coalesce(
        unpublished$do_path[match(filename, unpublished$filename)],
        do_path
      )
    )
}

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  print("Loading metadata table...")
  metadata <- tbl(sc, METADATA_TABLE) %>% collect()
  print(paste("Loaded", nrow(metadata), "records"))

  updated_metadata <- compute_metadata_updates(metadata)

  if (identical(metadata, updated_metadata)) {
    return(print("No updates needed"))
  }

  print("Updating metadata table...")
  copy_to(sc, updated_metadata, "tmp_new_meta", overwrite = TRUE)

  DBI::dbExecute(
    sc,
    paste0(
      "UPDATE ", METADATA_TABLE, " AS m ",
      "SET
        version_label = (
          SELECT ANY_VALUE(p.version_label)
          FROM tmp_new_meta p
          WHERE p.filename = m.filename
        ),
        do_path = (
          SELECT ANY_VALUE(p.do_path)
          FROM tmp_new_meta p
          WHERE p.filename = m.filename
        )
      WHERE m.filename IN (SELECT filename FROM tmp_new_meta)"
    )
  )

  DBI::dbExecute(sc, "DROP TABLE IF EXISTS tmp_new_meta")
  print("Done")
}
