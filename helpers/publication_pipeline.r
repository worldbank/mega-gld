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

# This function creates a project in the Metadata Editor by uploading the json file
create_project <- function(json_data, ME_API_KEY){
  url <- paste0(METADATA_API_BASE, "editor/create/survey")
  resp <- with_retry(function() httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    httr::timeout(60),
    body   = json_data,
    encode = "json"
  ))
  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")

  if (httr::status_code(resp) >= 300) {
    message("Dataset creation failed: ", parsed$message)
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
    overwrite  = 1,
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

publish_project<- function(project_id, ME_API_KEY, catalog_connection_id, classification = NA_character_, publish_metadata = TRUE, publish_thumbnail = TRUE, publish_resources = TRUE) {

  url <- paste0(METADATA_API_BASE, "jobs/publish_to_nada")

  access_policy <- if (!is.na(classification) && classification == "Confidential") "licensed" else "public"

  options <- list(
    overwrite     = "yes",
    published     = 1,
    access_policy = access_policy,
    repositoryid  = "GLD"
  )


  body <- list(
    project_id            = project_id,
    catalog_connection_id = catalog_connection_id,
    publish_metadata      = publish_metadata,
    publish_thumbnail     = publish_thumbnail,
    publish_resources     = publish_resources,
    options               = options
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
# MAGIC #### Functions specific to harmonized tables

# COMMAND ----------

# This function safely parses a NADA API JSON response body, falling back to raw text if parsing fails
parse_nada_response <- function(resp) {
  tryCatch(
    httr::content(resp, as = "parsed", encoding = "UTF-8"),
    error = function(e) httr::content(resp, as = "text", encoding = "UTF-8")
  )
}

# This function checks whether a NADA API response succeeded at both the HTTP and JSON-payload level
# (a "status": "error" body can come back with HTTP 200, so the HTTP status code alone is not reliable)
nada_request_ok <- function(resp, parsed) {
  httr::status_code(resp) == 200 &&
    is.list(parsed) &&
    (is.null(parsed$status) || identical(parsed$status, "success"))
}

# This function extracts a human-readable error message from a NADA API response
nada_error_message <- function(resp, parsed) {
  if (is.list(parsed) && !is.null(parsed$message)) {
    parsed$message
  } else {
    httr::content(resp, as = "text", encoding = "UTF-8")
  }
}

# This function POSTs a JSON body to a NADA API endpoint and returns the response alongside its parsed body
nada_post_json <- function(endpoint, NADA_API_KEY, body, timeout_secs = 60) {
  resp <- with_retry(function() httr::POST(
    paste0(MICRODATA_API_BASE, endpoint),
    httr::add_headers(`X-API-KEY` = NADA_API_KEY),
    httr::timeout(timeout_secs),
    body   = body,
    encode = "json"
  ))
  list(resp = resp, parsed = parse_nada_response(resp))
}

# This function creates a new table in the microdata catalog with title and description
create_table <- function(db_id, table_id, title, description, NADA_API_KEY) {
  result <- nada_post_json(
    paste0("tables/create_table/", db_id, "/", table_id),
    NADA_API_KEY,
    list(title = title, description = description)
  )

  if (!nada_request_ok(result$resp, result$parsed)) {
    message("Table creation failed [HTTP ", httr::status_code(result$resp), "]: ", nada_error_message(result$resp, result$parsed))
    return(NA)
  }

  message("Table created: ", table_id)
  TRUE
}

# This function uploads a data file to the NADA resumable-upload API in chunks and returns the resulting upload_id
upload_file_resumable <- function(file_path, NADA_API_KEY, chunk_size = 5 * 1024 * 1024) {
  file_size <- file.info(file_path)$size
  total_chunks <- as.integer(ceiling(file_size / chunk_size))
  filename <- basename(file_path)

  init <- nada_post_json(
    "uploads/init",
    NADA_API_KEY,
    list(filename = filename, total_size = file_size, total_chunks = total_chunks, chunk_size = chunk_size)
  )

  if (!nada_request_ok(init$resp, init$parsed) || is.null(init$parsed$upload_id)) {
    message("Resumable upload init failed: ", nada_error_message(init$resp, init$parsed))
    return(NA)
  }

  upload_id <- init$parsed$upload_id

  con <- file(file_path, "rb")
  on.exit(close(con))

  for (chunk_num in seq_len(total_chunks) - 1L) {
    read_size  <- if (chunk_num == total_chunks - 1) file_size - chunk_num * chunk_size else chunk_size
    chunk_data <- readBin(con, "raw", read_size)

    chunk_resp <- with_retry(function() httr::POST(
      paste0(MICRODATA_API_BASE, "uploads/chunk/", upload_id),
      httr::add_headers(
        `X-API-KEY`             = NADA_API_KEY,
        `Content-Type`          = "application/octet-stream",
        `X-Upload-Chunk-Number` = as.character(chunk_num),
        `X-Upload-Chunk-Size`   = as.character(length(chunk_data))
      ),
      httr::timeout(120),
      body = chunk_data
    ))
    chunk_parsed <- parse_nada_response(chunk_resp)

    if (!nada_request_ok(chunk_resp, chunk_parsed)) {
      message("Upload chunk ", chunk_num, " failed: ", nada_error_message(chunk_resp, chunk_parsed))
      return(NA)
    }

    message(sprintf("Uploaded chunk %d/%d", chunk_num + 1, total_chunks))
  }

  upload_id
}

# This function registers a resumable-uploaded file as the data source for a catalog table.
# Only needed once per file - re-registering would start the import over, so it must not be called
# again on a retry once import batches have already made progress.
register_table_upload <- function(db_id, table_id, upload_id, title, description, NADA_API_KEY) {
  register <- nada_post_json(
    paste0("tables/upload/", db_id, "/", table_id),
    NADA_API_KEY,
    list(upload_id = upload_id, title = title, description = description)
  )

  if (!nada_request_ok(register$resp, register$parsed)) {
    message("Table upload registration failed: ", nada_error_message(register$resp, register$parsed))
    return(NA)
  }

  TRUE
}

# This function runs the batch import to completion, following the server's has_more/last_processed_row
# progress until the whole file is imported. It is safe to call repeatedly/resume: each call just
# continues wherever the last successful batch left off, so no rows are re-imported or lost on retry.
run_import_loop <- function(db_id, table_id, NADA_API_KEY, max_rows = NULL, sleep_seconds = 1, max_batches = 1000) {
  has_more <- TRUE
  batch_count <- 0

  import_body <- list(db_id = db_id, table_id = table_id)
  if (!is.null(max_rows)) {
    import_body$max_rows <- max_rows
  }

  while (has_more && batch_count < max_batches) {
    if (batch_count > 0 && sleep_seconds > 0) Sys.sleep(sleep_seconds)

    import <- nada_post_json("tables/import", NADA_API_KEY, import_body, timeout_secs = 300)
    batch_count <- batch_count + 1
    parsed <- import$parsed

    if (!nada_request_ok(import$resp, parsed) || !is.list(parsed$progress)) {
      message("Batch import failed on batch ", batch_count, ": ", nada_error_message(import$resp, parsed))
      return(NA)
    }

    has_more <- isTRUE(parsed$progress$has_more)
    message(sprintf("Import batch %d: %s rows processed", batch_count, parsed$progress$total_rows_processed))
  }

  if (has_more) {
    message("Reached maximum batch limit of ", max_batches, " — import is incomplete")
    return(NA)
  }

  TRUE
}

# This function publishes a table: creates it, uploads the data file in resumable chunks, and imports it.
# It first tries to resume an import already in progress for this table_id (e.g. from a prior attempt that
# got partway through a large import before failing elsewhere) by calling the import loop directly, skipping
# create/upload entirely when that succeeds. This means a retry never re-uploads or re-imports rows that
# were already safely committed - it only redoes create+upload when there's genuinely nothing to resume.
publish_table_file <- function(db_id, table_id, file_path, title, description, NADA_API_KEY,
                                chunk_size = 5 * 1024 * 1024, max_rows = NULL) {
  if (isTRUE(run_import_loop(db_id, table_id, NADA_API_KEY, max_rows))) {
    message("Table published, import complete: ", table_id)
    return(TRUE)
  }

  table_created <- create_table(db_id, table_id, title, description, NADA_API_KEY)
  if (identical(table_created, NA)) {
    return(NA)
  }

  upload_id <- upload_file_resumable(file_path, NADA_API_KEY, chunk_size)
  if (identical(upload_id, NA)) {
    message("Table publish failed: upload did not complete")
    return(NA)
  }

  registered <- register_table_upload(db_id, table_id, upload_id, title, description, NADA_API_KEY)
  if (identical(registered, NA)) {
    return(NA)
  }

  import_complete <- run_import_loop(db_id, table_id, NADA_API_KEY, max_rows = max_rows)
  if (!isTRUE(import_complete)) {
    message("Table publish failed: import did not complete")
    return(NA)
  }

  message("Table published, import complete: ", table_id)
  TRUE
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

# This function records a published harmonized release in the version publication tracker table
record_published_version <- function(sc, filename, table_name, v_version, table_version, version_notes) {
  row <- tibble::tibble(
    filename      = filename,
    table_name    = table_name,
    v_version     = as.integer(v_version),
    table_version = as.integer(table_version),
    version_notes = version_notes
  )

  sparklyr::sdf_copy_to(sc, row, "tmp_tracker_row", overwrite = TRUE)
  DBI::dbExecute(sc, paste0("INSERT INTO ", TRACKER_TABLE, " SELECT * FROM tmp_tracker_row"))
  DBI::dbExecute(sc, "DROP TABLE IF EXISTS tmp_tracker_row")

  message("Recorded published version: ", filename, " (v_version=", v_version, ", table_version=", table_version, ")")
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
    return("")
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
