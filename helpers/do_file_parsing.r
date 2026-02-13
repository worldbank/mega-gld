# Databricks notebook source
library(stringr)
library(fs)
library(readr)

# Q: looks like find_do_files is always called with filename, simplify implementation?
# select * from `prd_csc_mega`.`sgld48`.`_ingestion_metadata` where filename is null or filename = ''

find_do_files <- function(dta_path, filename) {
  version_folder <- fs::path_dir(fs::path_dir(fs::path_dir(dta_path)))
  programs_dir  <- fs::path(version_folder, "Programs")

  if (!fs::dir_exists(programs_dir)) {
    return("")
  }

  do_files <- fs::dir_ls(programs_dir, glob = "*.do", type = "file")
  if (length(do_files) == 0) {
    return("")
  }

  base <- tolower(fs::path_file(do_files))
  fn   <- tolower(filename)

  # Scoring system to assess which do file is best
  score <- integer(length(do_files))

  # 1) basename contains filename 
  score <- score + 2 * as.integer(grepl(fn, base, fixed = TRUE))

  # 2) all.do
  score <- score + 3L * as.integer(grepl("(^|_)all\\.do$", base))

  # 3) exact filename.do
  score <- score + 4 * as.integer(base == paste0(fn, ".do"))

  best <- which(score == max(score))
  if (max(score) == 0L || length(best) != 1L) {
    warning("No .do file or multiple .do files detected for ", filename, "; none selected.")
    return("")
  }

  do_files[best]
}


extract_version_control <- function(text) {
  lower <- tolower(text)
  start_tag <- "<_version control_>"
  end_tag   <- "</_version control_>"

  start <- stringr::str_locate(lower, start_tag)[1]
  end   <- stringr::str_locate(lower, end_tag)[1]

  lines <- unlist(strsplit(text, "\n"))

  # case 1: proper start–end block
  if (!is.na(start) && !is.na(end) && end > start) {
    section_text <- substr(text, start, end - 1)
    section <- unlist(strsplit(section_text, "\n"))
    section <- section[!stringr::str_detect(tolower(section), stringr::fixed(start_tag))]

  # case 2: Missing start tag, but end tag exists
  } else if (!is.na(end)) {
    # find end line index
    end_idx <- which(stringr::str_detect(tolower(lines), end_tag))[1]

    # collect bullet lines above end tag
    section <- character()
    for (ln in rev(lines[seq_len(end_idx - 1)])) {
      s <- trimws(ln)
      if (startsWith(s, "*")) {
        section <- c(section, ln)
      } else if (s == "") {
        next
      } else {
        break
      }
    }
    section <- rev(section)

  # case 3: Start tag exists, but end tag missing
  } else if (!is.na(start) && is.na(end)) {
    # find the start line
    start_idx <- which(stringr::str_detect(tolower(lines), start_tag))[1]

    section <- character()
    for (ln in lines[(start_idx + 1):length(lines)]) {
      s <- trimws(ln)
      if (s == "") next

      if (startsWith(s, "*")) {
        section <- c(section, ln)
      } else if (!startsWith(s, "*")) {
        # stop when hitting non-bullet content
        break
      }
    }

  # case4: Neither tag exists
  } else {
    return(list())
  }

  # parse entries
  entries <- list()
  current <- NULL

  for (ln in section) {
    s <- trimws(ln)
    if (s == "") next

    if (startsWith(s, "*")) {
      # new bullet
      if (!is.null(current)) entries <- append(entries, current)
      current <- trimws(sub("^\\*", "", s))
    } else {
      # continuation line
      if (is.null(current)) {
        current <- s
      } else {
        current <- paste(current, s)
      }
    }
  }

  if (!is.null(current)) entries <- append(entries, current)

  names(entries) <- sprintf("V%02d", seq_along(entries))
  return(entries)
}

parse_do_file <- function(path) {
  text <- readr::read_file(path)
  extract_version_control(text)
}

extract_v_text <- function(raw) {
  if (is.na(raw)) return("")

  s <- trimws(as.character(raw))

  # Remove leading "Date:"
  s <- gsub("(?i)^\\s*date\\s*[:\\-]*\\s*", "", s, perl = TRUE)

  # handle first hyphen separators
  m <- regexpr("\\s-\\s", s)

  if (m > 0) {
    s <- trimws(sub(".*? -\\s*", "", s))
  }

  # Remove leading File:
  s <- gsub("(?i)^file:\\s*", "", s, perl = TRUE)

  # Remove surrounding brackets
  s <- gsub("^[\\[\\(]+", "", s)
  s <- gsub("[\\]\\)]+$", "", s)

  # Random leftover patterns
  s <- gsub("(?i)^\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}\\]\\s*file:\\s*\\[", "", s, perl = TRUE)

  # Normalize whitespace
  s <- gsub("\\s+", " ", s)
  s <- trimws(s)

  # Translate punctuation
  translations <- c("；"=";", "："=":", "，"=",", "（"="(", "）"=")",
                    "“"="\"", "”"="\"", "《"="", "》"="", "<" = "", "]" = "", "[" = "")
  for (k in names(translations)) {
    s <- gsub(k, translations[[k]], s, fixed = TRUE)
  }

  return(s)
}
select_latest_version <- function(row, v_cols) {
  # Iterate over version columns in reverse order
  for (col in rev(v_cols)) {
    val <- row[[col]]

    if (is.null(val) || is.na(val) ||
        !nzchar(trimws(val)) ||
        trimws(tolower(val)) == "na") {
      next
    }

    cleaned <- extract_v_text(val)
    cleaned <- trimws(cleaned)

    if (!nzchar(cleaned) || grepl("^description of changes$", cleaned, ignore.case = TRUE)) next

    return(cleaned)
  }
  return("")
}


make_version_label <- function(txt) {
  if (!is.character(txt) || !nzchar(trimws(txt))) return("")
  trimws(txt)
}
