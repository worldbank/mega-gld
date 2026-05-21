# Databricks notebook source
library(httr)
library(fs)
library(zip)
library(sparklyr)
library(DBI)

# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
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
upload_microdata_file <- function(project_id, file_path, ME_API_KEY, description = NULL) {
  stata_ver  <- get_stata_version(file_path)
  base_name  <- tools::file_path_sans_ext(basename(file_path))
  new_name   <- paste0(base_name, "_Stata", stata_ver, ".dta")
  upload_path <- file.path(tempdir(), new_name)
  file.copy(file_path, upload_path, overwrite = TRUE)
  on.exit(unlink(upload_path))

  url <- paste0(METADATA_API_BASE, "jobs/import_microdata/", project_id)
  body <- list(
    file       = httr::upload_file(upload_path),
    overwrite  = 0,
    store_data = "store"
  )
  if (!is.null(description) && nzchar(trimws(description))) {
    body$description <- description
  }
  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-Key` = ME_API_KEY),
    httr::timeout(300),
    body = body,
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
      access_policy = "public",
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
# MAGIC #### Functions specific to long-wide tables

# COMMAND ----------

# This function creates a new table in the microdata catalog with title and description
create_table <- function(db_id, table_id, title, description, NADA_API_KEY) {
  url <- paste0(MICRODATA_API_BASE, "tables/create_table/", db_id, "/", table_id)

  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = NADA_API_KEY, `Accept` = "application/json"),
    httr::timeout(60),
    body   = list(title = title, description = description),
    encode = "json"
  ))

  raw_text <- httr::content(resp, as = "text", encoding = "UTF-8")

  if (httr::status_code(resp) >= 300) {
    message("Table creation failed [HTTP ", httr::status_code(resp), "]: ", raw_text)
    return(NA)
  }

  parsed <- jsonlite::fromJSON(raw_text, simplifyVector = FALSE)
  message("Table created: ", parsed$message)
  TRUE
}

# This function uploads a zipped CSV file to the catalog table created by create_table()
upload_table_file <- function(db_id, table_id, file_path, title, description, NADA_API_KEY) {
  url <- paste0(MICRODATA_API_BASE, "tables/upload/", db_id, "/", table_id)

  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = NADA_API_KEY),
    httr::timeout(300),
    body = list(
      file        = httr::upload_file(file_path),
      title       = title,
      description = description
    ),
    encode = "multipart"
  ))

  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")

  if (httr::status_code(resp) >= 300) {
    message("Table upload failed: ", parsed$message)
    return(NA)
  }

  message("Table uploaded, import status: ", parsed$import_status)
  parsed$import_status
}


# This function attaches a catalog table to a study using the study IDNO
attach_table_to_study <- function(db_id, table_id, idno, NADA_API_KEY) {
  url <- paste0(MICRODATA_API_BASE, "tables/attach_to_study")

  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = NADA_API_KEY),
    httr::timeout(60),
    body   = list(db_id = db_id, table_id = table_id, idno = idno),
    encode = "json"
  ))

  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")


  if (httr::status_code(resp) >= 300) {
    message("Table attach failed: ", parsed$message)
    return(NA)
  }

  isTRUE(parsed$result)
}


# COMMAND ----------

# MAGIC %md
# MAGIC
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
update_metadata <- function(filename) {
  DBI::dbExecute(
    sc,
    paste0(
      "UPDATE ", METADATA_TABLE, "
       SET published = TRUE
       WHERE filename = '", filename, "'"
    )
  )
  message("Updated metadata for: ", filename)
}


get_project_id_by_idno <- function(idno, ME_API_KEY) {
  url <- paste0(METADATA_API_BASE, "editor/", idno)
  
  resp <- httr::GET(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY)
  )
  
  if (httr::status_code(resp) >= 300) {
    return(NA)
  }
  
  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  as.integer(parsed$project$id)
}
