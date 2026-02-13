# Databricks notebook source
library(purrr)
library(dplyr)

# COMMAND ----------

# MAGIC %run "./do_file_parsing"

# COMMAND ----------

# MAGIC %run "./txt_parsing"

# COMMAND ----------

compute_metadata_updates <- function(metadata) {
  unpublished <- metadata %>%
    filter(
      published == FALSE,
      is.na(do_path)
    )

  print(paste("Found", nrow(unpublished), "unpublished records missing do_path"))

  if (nrow(unpublished) > 0) {

    # ---- find do files  ----

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
  }

  # ---- compute data classification ----

  unpublished_class <- metadata %>%
    filter(
      published == FALSE,
      is.na(classification) | trimws(classification) == ""
    )

  print(paste("Found", nrow(unpublished_class), "unpublished records missing data classification"))

  if (nrow(unpublished_class) > 0) {
    print("Finding txt files...")
    unpublished_class <- unpublished_class %>%
      mutate(txt_path = map2_chr(dta_path, filename, ~ suppressWarnings(find_txt_files(.x, .y))))

    missing_txt <- unpublished_class %>%
      filter(is.na(txt_path) | !nzchar(txt_path)) %>%
      pull(filename)

    if (length(missing_txt) > 0) {
      warning(
        paste0(
          "No txt file found for ", length(missing_txt), " file(s):\n",
          paste(missing_txt, collapse = "\n")
        ),
        call. = FALSE
      )
    }

    print("Detecting classification...")
    
    unpublished_class <- unpublished_class %>%
      mutate(
        classification_new = if_else(
          is.na(txt_path) | !nzchar(txt_path),
          NA_character_,
          map2_chr(txt_path, filename, ~ suppressWarnings(detect_classification(.x, .y)))
        )
      )

    not_detected <- unpublished_class %>%
      filter(
        !is.na(txt_path), nzchar(txt_path),
        is.na(classification_new) | !nzchar(classification_new)
      ) %>%
      pull(filename)

    if (length(not_detected) > 0) {
      warning(
        paste0(
          "Txt found but classification not detected for ", length(not_detected), " file(s):\n",
          paste(not_detected, collapse = "\n")
        ),
        call. = FALSE
      )
    }
    
    unpublished_class <- unpublished_class %>%
      mutate(classification = dplyr::coalesce(classification_new, classification)) %>%
      select(-classification_new)
  }

  # --- add to metadata table ---
  metadata %>%
  dplyr::mutate(
    do_path = dplyr::coalesce(
      unpublished$do_path[match(filename, unpublished$filename)],
      do_path
    ),
    classification = dplyr::coalesce(
      unpublished_class$classification[match(filename, unpublished_class$filename)],
      classification
    ),
    version_label = if (exists("parsed_df") && nrow(parsed_df) > 0) {
      dplyr::coalesce(
        parsed_df$version_label[match(filename, parsed_df$filename)],
        version_label
      )
    } else {
      version_label
    }
  )
}
