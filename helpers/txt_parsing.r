# Databricks notebook source
library(purrr)
library(dplyr)
library(stringr)
library(fs)
library(readr)

find_txt_files <- function(harmonized_path, filename) {
  version_folder <- path_dir(path_dir(path_dir(harmonized_path)))
  tech_dir <- path(version_folder, "Doc/Technical")
  doc_dir  <- path(version_folder, "Doc")
  
  if (dir_exists(tech_dir)) {
    programs_dir <- tech_dir
  } else if (dir_exists(doc_dir)) {
    programs_dir <- doc_dir
  } else {
    warning("No Doc or Technical directory found for ", filename, "; no txt file retrieved")
    return("")
  }

  txt_files <- dir_ls(programs_dir, regexp = "(?i)(readme|where_is_this_data_from)\\.txt$")

  if (length(txt_files) == 1) {
    as.character(txt_files[1])
  } else {
    warning("No txt file found for ", filename)
    ""
  }
}

detect_classification <- function(path, filename) {
  if (is.na(path) || !file.exists(path)) {
    warning("txt file missing for ", filename)
    return("")
  }

  txt <- tryCatch(
    read_file(path),
    error = function(e) ""
  )
  
  if (str_detect(txt, regex("Classification[=:]\\s*(OFFICIAL_USE|OFFICIAL-USE|OFFICIAL USE)", ignore_case = TRUE))) {
    return("Official Use")
  }

  if (str_detect(txt, regex("Classification[=:]\\s*CONFIDENTIAL", ignore_case = TRUE))) {
      return("Confidential")
  }

  has_confidential <- str_detect(txt, regex("confidential|needs to approve", ignore_case = TRUE))
  has_official_use <- str_detect(txt, regex("official use|share it freely|public domain|publicly available|freely available|internal use only|downloadable freely|shared freely|free to use|no specific terms", ignore_case = TRUE))

  if (has_confidential) {
    "Confidential"
  } else if (has_official_use) {
    "Official Use"
  } else {
    warning("Fail: could not detect data classification from text for ", filename)
    ""
  }
}


