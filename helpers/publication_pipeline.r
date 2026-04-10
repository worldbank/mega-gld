# Databricks notebook source
library(httr)
library(fs)
library(zip)
library(sparklyr)
library(DBI)

# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

# MAGIC %run "./ai_description"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

if (!exists("get_ai_descriptions")) {
  source("helpers/ai_description.r")
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions handling API calls

# COMMAND ----------

# Retries a function call up to max_attempts times, waiting wait_secs between each
with_retry <- function(f, max_attempts = 3, wait_secs = 60) {
  for (attempt in seq_len(max_attempts)) {
    result <- tryCatch(f(), error = function(e) e)
    if (!inherits(result, "error")) return(result)
    if (attempt < max_attempts) {
      message(sprintf("Attempt %d/%d failed: %s — retrying in %ds...",
                      attempt, max_attempts, conditionMessage(result), wait_secs))
      Sys.sleep(wait_secs)
    }
  }
  stop(sprintf("All %d attempts failed: %s", max_attempts, conditionMessage(result)))
}

# This function creates an project in the Metadata Editor by uploading the json file
create_project <- function(json_data, ME_API_KEY){
  url <- paste0(METADATA_API_BASE, "editor/create/survey")
  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    httr::timeout(60),
    body = json_data,
    encode = "json"
  ))
  
  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  
  if (httr::status_code(resp) >= 300) {
    message("Dataset creation failed ", parsed$message)
    return(NA)
  }
  
  parsed$id
}


# This function uploads the microdata file to the project created using create_project(), and generates statistics for microdata variables
upload_microdata_file <- function(project_id, file_path, ME_API_KEY) {
  stata_ver <- get_stata_version(file_path)
  base_name <- tools::file_path_sans_ext(basename(file_path))
  zipname   <- paste0(base_name, "_Stata", stata_ver, ".zip")
  zipfile   <- make_zip(zipname, file_path, dirname(file_path))

  url <- paste0(METADATA_API_BASE, "jobs/import_microdata/", project_id)
  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-Key` = ME_API_KEY),
    httr::timeout(300),
    body = list(
      file       = httr::upload_file(zipfile),
      overwrite  = 0,
      store_data = "store"
    ),
    encode = "multipart"
  ))

  if (httr::status_code(resp) >= 300) {
    message("Microdata upload failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  httr::content(resp, as = "parsed")$file_id
}

# This function creates an external resource in the Metadata Editor project by uploading a file through the Metadata API resources endpoint
create_resource <- function(project_id, resource_body, file_path, ME_API_KEY) {
  url <- paste0(METADATA_API_BASE, "resources/", project_id)
  body <- c(
    resource_body,
    list(file = httr::upload_file(file_path))
  )
  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    httr::timeout(120),
    body   = body,
    encode = "multipart"
  ))
  
  if (httr::status_code(resp) >= 300) {
    message("Resource creation failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  
  parsed <- httr::content(resp, as = "parsed")
  if (!is.null(parsed$id)) parsed$id else TRUE
}


publish_project<- function(project_id, ME_API_KEY, catalog_connection_id, publish_metadata = TRUE, publish_thumbnail = TRUE, publish_resources = TRUE) {
  
  url <- paste0(METADATA_API_BASE, "jobs/publish_to_nada")

  body <- list(
    project_id           = project_id,
    catalog_connection_id = catalog_connection_id,
    publish_metadata     = publish_metadata,
    publish_thumbnail    = publish_thumbnail,
    publish_resources    = publish_resources,
    options              = list(
      overwrite = "yes",
      published = 1,
      access_policy = "direct",
      repositoryid = "GLD"
    )
  )

  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    body   = body,
    encode = "json"
  )

  parsed     <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  status_ok  <- httr::status_code(resp) < 300 && identical(parsed$status, "success")

  if (!status_ok) {
    cat("Dataset publish failed: ", jsonlite::toJSON(parsed, auto_unbox = TRUE))
    cat("project_id: ",            project_id)
  }


  list(
    url           = url,
    status_code   = httr::status_code(resp),
    success       = status_ok,
    job_uuid      = parsed$job_uuid,
    job           = parsed$job,
    error_message = parsed$job$error_message,
    response      = parsed,
    payload       = body
  )
}


# This function fetches the project JSON, injects variable labels, and updates the project. It is currently used to publish the harmonized tables CSV.
update_project_with_variables <- function(project_id, table_name, sc, ME_API_KEY) {
  # 1. Fetch the current project JSON
  url_get <- paste0(METADATA_API_BASE, "editor/json/", project_id)
  
  resp_get <- httr::GET(
    url_get,
    httr::add_headers(`X-API-KEY` = ME_API_KEY)
  )
  
  if (httr::status_code(resp_get) >= 300) {
    message("Failed to fetch project JSON: ", httr::content(resp_get, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  
  json_obj <- httr::content(resp_get, as = "parsed", encoding = "UTF-8")
  
  # 2. Get column metadata from Databricks
  col_metadata <- DBI::dbGetQuery(sc, paste0("DESCRIBE TABLE ", table_name))
  
  # 3. Update the labels in existing variables
  if (!is.null(json_obj$variables) && length(json_obj$variables) > 0) {
    for (i in seq_along(json_obj$variables)) {
      var_name <- json_obj$variables[[i]]$name
      matching_col <- col_metadata[col_metadata$col_name == var_name, ]
      if (nrow(matching_col) > 0 && !is.na(matching_col$comment[1]) && nzchar(matching_col$comment[1])) {
        json_obj$variables[[i]]$labl <- matching_col$comment[1]
      }
    }
  }
  
  # 4. Send updated JSON back
  url_update <- paste0(METADATA_API_BASE, "editor/update/survey/", project_id)
  
  resp_update <- httr::POST(
    url_update,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    body = json_obj,
    encode = "json"
  )
  
  if (httr::status_code(resp_update) >= 300) {
    message("Project update failed: ", httr::content(resp_update, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  
  message("Successfully updated variable labels for project ", project_id)
  return(TRUE)
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions handling external resources
# MAGIC

# COMMAND ----------

get_author <- function(row) {
  if (!is.null(row$producers_name) &&
      !is.na(row$producers_name) &&
      nzchar(trimws(row$producers_name))) {
    row$producers_name
  } else {
    paste("National Statistical Offices of", row$nation_name)
  }
}


make_zip <- function(zipname, files_abs, root_dir) {
  zipfile   <- file.path(tempdir(), zipname)
  rel_files <- fs::path_rel(files_abs, start = root_dir)
  zip::zip(zipfile, files = rel_files, root = root_dir)
  zip_contents <- zip::zip_list(zipfile)$filename
  message(sprintf("Created ZIP %s with %d files", basename(zipfile), length(zip_contents)))
  message(sprintf("ZIP contents: %s", paste(zip_contents, collapse = ", ")))
  zipfile
}


log_resource <- function(kind, res, idno) {
  if (is.na(res)) {
    message(kind, " resource creation failed for ", idno)
  } else if (isTRUE(res)) {
    message(kind, " resource created for ", idno, ", but no id returned")
  } else {
    message(kind, " resource created for ", idno, " (resource_id = ", res, ")")
  }
}



upload_resource <- function(project_id, file_path, resource_body,
                            ME_API_KEY, label, idno) {
  res <- create_resource(project_id, resource_body,
                         file_path = file_path, ME_API_KEY)
  log_resource(paste0(label, " (", basename(file_path), ")"), res, idno)
  res
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Other helper functions

# COMMAND ----------

# this function gets the stata version from the dta file
get_stata_version <- function(dta_path) {
  raw <- readBin(dta_path, what = "raw", n = 80)
  first_char <- rawToChar(raw[1])
  
  if (first_char == "<") {
    # Only convert bytes up to the first null byte
    nul_pos <- which(raw == as.raw(0))
    end_pos <- if (length(nul_pos) > 0) nul_pos[1] - 1 else length(raw)
    header_str <- rawToChar(raw[1:end_pos])
    m <- regmatches(header_str, regexpr("(?<=<release>)\\d+", header_str, perl = TRUE))
    if (length(m) == 0) return("unknown")
    
    format_to_version <- c(
      `117` = "13",
      `118` = "14",
      `119` = "15"
    )
    version <- format_to_version[m]
    return(if (is.na(version)) "unknown" else version)
  }
  
  code <- as.integer(raw[1])
  format_to_version <- c(
    `102` = "1",
    `103` = "2",
    `104` = "3",
    `105` = "4",
    `108` = "6",
    `110` = "7",
    `111` = "7se",
    `112` = "8",
    `113` = "8",
    `114` = "10",
    `115` = "12"
  )
  version <- format_to_version[as.character(code)]
  if (is.na(version)) {
    message("Unknown Stata format byte: ", code)
    return("unknown")
  }
  version
}

# This function updates the published flag in _ingestion_metadata
update_metadata <- function(fname_base) {
  DBI::dbExecute(
    sc,
    paste0(
      "UPDATE ", METADATA_TABLE, "
       SET published = TRUE
       WHERE fname_base = '", fname_base, "'"
    )
  )
  message("Updated metadata for: ", fname_base)
}

