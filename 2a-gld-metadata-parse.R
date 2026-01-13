library(purrr)
library(dplyr)
library(haven)
library(sparklyr)
library(stringr)
library(fs)

root_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Documents"

sc <- spark_connect(method = "databricks")

target_schema  <- "prd_csc_mega.sgld48"
metadata_table <- paste0(target_schema, "._ingestion_metadata")

metadata <- tbl(sc, metadata_table) %>% collect()

unpublished <- metadata %>% 
  filter(published == FALSE,
    !is.na(version_label),
    nzchar(trimws(version_label))
  )


# --- find do files paths  ---
find_do_files <- function(harmonized_path, filename) {
  version_folder <- path_dir(path_dir(path_dir(harmonized_path)))
  programs_dir  <- path(version_folder, "Programs")

  if (!dir_exists(programs_dir)) {
    return("")
  }
  
  do_files <- as.character(dir_ls(programs_dir, glob = "*.do"))
  n <- length(do_files)
  
  if (n == 1) {
    return(do_files)
  }

  if (n == 2) {
    contains_name <- do_files[grepl(filename, basename(do_files), ignore.case = TRUE)]
    if (length(contains_name) == 1) {
      return(contains_name)
    }

    all_do <- do_files[grepl("all\\.do$", basename(do_files), ignore.case = TRUE)]
    if (length(all_do) == 1) {
      return(all_do)
    }

    no_suffix <- do_files[basename(do_files) == paste0(filename, ".do")]
    if (length(no_suffix) == 1) {
      return(no_suffix)
    }

    warning("No .do file or mupltiple .do files detected for ", filename,"; none selected.")
    return("")
  }
  
  warning("No .do file or mupltiple .do files detected for ", filename,"; none selected.")
  return("")
}

unpublished <- unpublished %>%
  mutate(do_path = map2_chr(dta_path, filename, find_do_files))

# --- helpers to extract versioning text ---

extract_single_field <- function(text, field_label) {
  pattern <- sprintf("<_%s_>\\s*(.*)", stringr::str_replace_all(field_label, "([.|()\\^{}+$*?])", "\\\\\\1"))
  lines <- unlist(strsplit(text, "\n", fixed = TRUE))

  for (ln in lines) {
    m <- stringr::str_match(ln, pattern)
    if (!is.na(m[1,2])) {
      val <- m[1,2]
      val <- stringr::str_replace(val, "</_.*?_>", "")
      return(trimws(val))
    }
  }
  return(NULL)
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
    section_text <- substr(text, start, end)
    section <- unlist(strsplit(section_text, "\n"))

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
    start_idx <- which(stringr::str_detect(lower, start_tag))[1]

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



# --- parse the do files ---

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


# --- helpers to compute version ---

extract_v_text <- function(raw) {
  if (is.na(raw)) return("")

  s <- trimws(as.character(raw))

  # Remove <_version control_> tag variants
  s <- gsub("(?i)<\\s*_?version control_?\\s*>", "", s, perl = TRUE)
  s <- trimws(s)

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

# --- compute version ---

v_cols <- grep("^V\\d{2}$", names(parsed_df), value = TRUE)

parsed_df$latest_v_text <- apply(parsed_df[v_cols], 1, function(x) {
  select_latest_version(as.list(x), v_cols)
})

parsed_df$version_label <- sapply(parsed_df$latest_v_text, make_version_label)

# --- add to metadata table ---

metadata <- metadata %>%
  mutate(
    version_label = coalesce(parsed_df$version_label[ match(filename, parsed_df$filename) ],version_label),
    do_path = coalesce(unpublished$do_path[ match(filename, unpublished$filename) ],do_path)
  )



copy_to(sc, metadata, "tmp_new_meta", overwrite = TRUE)

DBI::dbExecute(
  sc,
  paste0(
    "UPDATE ", metadata_table, " AS m ",
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

