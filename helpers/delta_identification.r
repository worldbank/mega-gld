# Databricks notebook source
library(purrr)
library(dplyr)
library(stringr)

list_dta_files <- function(paths) {
  unlist(map(paths, ~ list.files(.x, pattern = "\\.dta$", full.names = TRUE)))
}

identify_all_versions <- function(root_dir) {
  country_dirs <- list.dirs(root_dir, recursive = FALSE)
  dataset_dirs <- unlist(lapply(country_dirs, list.dirs, recursive = FALSE))
  version_dirs <- unlist(lapply(dataset_dirs, function(d) {
    list.dirs(d, recursive = FALSE)
  }))

  harmonized_paths <- file.path(version_dirs, "Data", "Harmonized")
  harmonized_paths <- harmonized_paths[dir.exists(harmonized_paths)]

  all_dta_files <- list_dta_files(harmonized_paths)

  if (length(all_dta_files) == 0) {
    return(tibble())
  }

  parsed <- bind_rows(lapply(all_dta_files, parse_metadata_from_filename))
  print(paste("New tables filtered:", nrow(parsed),"covering", n_distinct(parsed$country), "countries."))

  parsed
}

parse_metadata_from_filename <- function(path) {
  fname <- basename(dirname(dirname(dirname(path))))

  tibble(
    filename   = fname,
    dta_path = path,
    country    = str_extract(fname, "^[A-Z]+(?=_)"),
    year       = str_match(fname, "^[A-Z]+_([0-9]{4})")[ ,2],
    quarter    = str_match(fname, "-(Q[1-4])_")[ ,2],
    survey     = str_match(fname, "(?i)^[A-Z]+_[0-9]{4}_(.+?)_v")[ ,2] %>% str_replace("-(Q[1-4])$", ""),
    M_version  = str_match(fname, "(?i)_v([0-9]+)_m")[ ,2] %>% as.integer(),
    A_version  = str_match(fname, "(?i)_m_v([0-9]+)_a")[ ,2] %>% as.integer()
  )
}

make_table_name <- function(path) {
  nm <- basename(path)
  nm <- sub("\\.dta$", "", nm, ignore.case = TRUE)
  nm <- sub("(?i)_V[0-9]+_M_V[0-9]+_A.*$", "", nm, perl = TRUE)
  nm <- gsub("[^[:alnum:]]", "_", nm)
  nm <- gsub("_+", "_", nm)

  tolower(nm)
}


