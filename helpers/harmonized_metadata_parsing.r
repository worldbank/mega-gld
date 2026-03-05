# Databricks notebook source
library(dplyr)
library(purrr)
library(stringr)
library(tibble)

get_unique_vec <- function(tbl_data, col_name) {
  tbl_data %>%
    select(all_of(col_name)) %>%
    distinct() %>%
    collect() %>%
    pull(col_name)
}

get_year_range_chr <- function(tbl_data) {
  year_summary <- tbl_data %>%
    mutate(year_int = as.integer(year)) %>%
    summarise(
      min_year = min(year_int, na.rm = TRUE),
      max_year = max(year_int, na.rm = TRUE)
    ) %>%
    collect()

  min_year <- year_summary$min_year[1]
  max_year <- year_summary$max_year[1]

  if (!is.finite(min_year) || !is.finite(max_year)) {
    ""
  } else if (min_year == max_year) {
    as.character(min_year)
  } else {
    paste0(min_year, "-", max_year)
  }
}

get_version <- function(tbl_metadata, table_name_value) {
  is_ouo_table <- grepl("_ouo$", table_name_value)

  version_column <- if (is_ouo_table) {
    "stacked_ouo_published_version"
  } else {
    "stacked_all_published_version"
  }

  metadata_df <- tbl_metadata %>%
    filter(table_name == table_name_value) %>%
    collect()

  last_version <- max(as.integer(metadata_df[[version_column]]),na.rm = TRUE)

  if (is.na(last_version) || is.infinite(last_version)) {1} else {last_version + 1}
}

build_update_label <- function(updates_df) {
  if (nrow(updates_df) == 0) {
    return("")
  }

  label_parts <- updates_df %>%
    mutate(
      country = as.character(country),
      year = as.character(year)
    ) %>%
    group_by(country) %>%
    summarise(
      years = paste(sort(unique(year)), collapse = ", "),
      .groups = "drop"
    ) %>%
    arrange(country) %>%
    mutate(label = paste0(country, " ", years)) %>%
    pull(label)

  paste0("Updated data for ", paste(label_parts, collapse = "; "))
}

get_version_label <- function(sc, tbl_metadata, table_name_value) {
  is_ouo_table <- grepl("_ouo$", table_name_value)

  published_table_version_column <- if (is_ouo_table) {
    "stacked_ouo_published_table_version"
  } else {
    "stacked_all_published_table_version"
  }

  table_version_column <- if (is_ouo_table) {
    "stacked_ouo_table_version"
  } else {
    "stacked_all_table_version"
  }

  metadata_df <- tbl_metadata %>%
    filter(table_name == table_name_value) %>%
    collect()

  last_published_version <- max(
    as.integer(metadata_df[[published_table_version_column]]),
    na.rm = TRUE
  )

  if (is.infinite(last_published_version) || is.na(last_published_version)) {
    return("")
  }

  full_table_name <- paste0(TARGET_SCHEMA, ".", table_name_value)

  history_df <- sparklyr::sdf_sql(
    sc, paste0("DESCRIBE HISTORY ", full_table_name)) %>%
    arrange(desc(version)) %>%
    select(version) %>%
    collect()

  current_table_version <- as.integer(history_df$version[1])

  if (is.na(current_table_version) || current_table_version <= last_published_version) {
    return("")
  }

  updates_df <- metadata_df %>%
    filter(
      metadata_df[[table_version_column]] > last_published_version &
      metadata_df[[table_version_column]] <= current_table_version
    ) %>%
    select(country, year) %>%
    distinct()

  build_update_label(updates_df)
}


build_harmonized_metadata <- function(
  sc,
  tbl_metadata,
  tbl_all,
  tbl_ouo
) {
  table_names <- c("gld_harmonized_all", "gld_harmonized_ouo")

  data_tables <- list(
    gld_harmonized_all = tbl_all,
    gld_harmonized_ouo = tbl_ouo
  )

  tibble(
    table_name      = table_names,
    household_level = TRUE,
    country         = map(table_names, ~ get_unique_vec(data_tables[[.x]], "countrycode")),
    year            = map_chr(table_names, ~ get_year_range_chr(data_tables[[.x]])),
    quarter         = 'NA', #This variable is recoded as NA string since the quarter variable cannot be null for the stacking logic.
    survey          = NA_character_,
    V_version       = map_int(table_names, ~ get_version(tbl_metadata, .x)),
    version_label   = map_chr(table_names, ~ get_version_label(sc, tbl_metadata, .x)),
    classification  = if_else(grepl("_ouo$", table_names), "Official Use Only", "Confidential"),
    filename        = paste0(toupper(table_names), "_V", V_version),
    published       = FALSE,
    survey_extended = NA_character_,
    geog_coverage   = GEOG_COVERAGE_HARMONIZED,
    producers_name  = PRODUCERS_NAME_HARMONIZED
  )
}
