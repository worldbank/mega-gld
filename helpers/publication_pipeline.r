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

## This function creates a project in the Metadata Editor by uploading the json file
create_dataset <- function(json_data, ME_API_KEY){
  url <- paste0(METADATA_API_BASE, "editor/create/survey")
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    body = json_data,
    encode = "json"
  )
  
  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  
  if (httr::status_code(resp) >= 300) {
    message("Dataset creation failed ", parsed$message)
    return(NA)
  }
  
  parsed$id
}

## This function uploads the microdata file to the project created using create_dataset, and generates statistics for microdata variables
upload_microdata_file <- function(project_id, file_path, ME_API_KEY){
  url <- paste0(METADATA_API_BASE, "jobs/import_microdata/", project_id)
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-Key` = ME_API_KEY),
    body = list(
      file = httr::upload_file(file_path),
      overwrite = 0,
      store_data = "store"
    ),
    encode = "multipart"
  )
  
  if (httr::status_code(resp) >= 300) {
    message("Microdata upload failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  httr::content(resp, as = "parsed")$file_id
  
}



## This function uploads the microdata file to the project created using create_dataset, chunked
upload_microdata_file_chunked <- function(project_id, file_path, ME_API_KEY, chunk_size_mb = 500) {
  file_size <- file.info(file_path)$size
  file_name <- basename(file_path)
  upload_id <- paste0("upload_", project_id, "_", as.integer(Sys.time()))
  
  chunk_size_bytes <- chunk_size_mb * 1024 * 1024
  total_chunks <- ceiling(file_size / chunk_size_bytes)
  
  message(sprintf("Uploading %s (%.2f GB) in %d chunks", file_name, file_size / 1024^3, total_chunks))
  
  con <- file(file_path, "rb")
  on.exit(close(con))
  
  for (chunk_index in 0:(total_chunks - 1)) {
    chunk_data <- readBin(con, "raw", n = chunk_size_bytes)
    chunk_temp <- tempfile()
    writeBin(chunk_data, chunk_temp)
    
    is_final <- as.integer(chunk_index == total_chunks - 1)
    
    url <- paste0(METADATA_API_BASE, "upload/", project_id)
    resp <- httr::POST(
      url,
      httr::add_headers(
        `X-API-KEY` = ME_API_KEY,
        `X-Chunk-Index` = chunk_index,
        `X-Total-Chunks` = total_chunks,
        `X-Upload-ID` = upload_id,
        `X-File-Name` = file_name,
        `X-File-Size` = file_size,
        `X-Is-Final-Chunk` = is_final
      ),
      body = list(chunk = httr::upload_file(chunk_temp)),
      encode = "multipart"
    )
    
    unlink(chunk_temp)
    
    if (httr::status_code(resp) >= 300) {
      message("Chunk upload failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
      return(NA)
    }
    
    message(sprintf("Chunk %d/%d complete (%.1f%%)", 
                    chunk_index + 1, total_chunks, 
                    ((chunk_index + 1) / total_chunks) * 100))
  }
  
  # Parse the final response to get file_id
  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  
  message("Upload complete")
  
  # Return file_id if available, otherwise NA
  if (!is.null(parsed$file_id)) {
    return(parsed$file_id)
  } else {
    message("Warning: No file_id returned from API")
    return(NA)
  }
}


## This function creates External Resources in the Metadata Editor project
create_resource <- function(project_id, resource_body, file_path, ME_API_KEY) {
  url <- paste0(METADATA_API_BASE, "resources/", project_id)
  body <- c(
    resource_body,
    list(file = httr::upload_file(file_path))
  )
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = ME_API_KEY),
    body   = body,
    encode = "multipart"
  )
  
  if (httr::status_code(resp) >= 300) {
    message("Resource creation failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  parsed <- httr::content(resp, as = "parsed")
  if (!is.null(parsed$id)) parsed$id else TRUE
}

## This function creates zip files for external resources
make_zip <- function(zipname, files_abs, root_dir) {
  zipfile   <- file.path(tempdir(), zipname)
  rel_files <- fs::path_rel(files_abs, start = root_dir)
  zip::zip(zipfile, files = rel_files, root = root_dir)
  zip_contents <- zip::zip_list(zipfile)$filename
  message(sprintf("Created ZIP %s with %d files", basename(zipfile), length(zip_contents)))
  message(sprintf("ZIP contents: %s", paste(zip_contents, collapse = ", ")))
  zipfile
}

## This function handles logging of messages for external resources
log_resource <- function(kind, res, idno) {
  if (is.na(res)) {
    message(kind, " resource creation failed for ", idno)
  } else if (isTRUE(res)) {
    message(kind, " resource created for ", idno, ", but no id returned")
  } else {
    message(kind, " resource created for ", idno, " (resource_id = ", res, ")")
  }
}

## This function identifies technical documentation and questionnaires and uploads them as zipped files 
handle_doc_resources <- function(project_id, idno, dta_path, ME_API_KEY, row) {
  doc_root <- path_dir(path_dir(path_dir(dta_path)))
  doc_dir  <- path(doc_root, "Doc")
  
  if (!dir_exists(doc_dir)) {
    message("No Doc folder, skipping.")
    return()
  }
  
  tech_dir  <- path(doc_dir, "Technical")
  quest_dir <- path(doc_dir, "Questionnaires")
  tech_exists  <- dir_exists(tech_dir)
  quest_exists <- dir_exists(quest_dir)
  
  author <- if (!is.null(row$producers_name) && !is.na(row$producers_name) && nzchar(trimws(row$producers_name))) {
    row$producers_name
  } else {
    paste("National Statistical Offices of", row$nation_name)
  }
  
  if (tech_exists) {
    tech_files <- dir_ls(tech_dir, recurse = TRUE, type = "file")
    
    if (length(tech_files) > 0) {
      zipname <- paste0("Technical_", idno, ".zip")
      zipfile <- make_zip(zipname, tech_files, tech_dir)
      
      resource_body <- list(
        dctype      = "doc/tec",
        dcformat    = "application/zip",
        title       = "Technical Documents",
        author      = author,
        description = paste0(zipname, " includes the following files: ", paste(basename(tech_files), collapse = ", "))
      )
      
      res <- create_resource(project_id, resource_body, file_path = zipfile, ME_API_KEY)
      log_resource("Technical documentation", res, idno)
    }
  }
  
  if (quest_exists) {
    quest_files <- dir_ls(quest_dir, recurse = TRUE, type = "file")
    
    if (length(quest_files) > 0) {
      zipname <- paste0("Questionnaires_", idno, ".zip")
      zipfile <- make_zip(zipname, quest_files, quest_dir)
      
      resource_body <- list(
        dctype      = "doc/qst",
        dcformat    = "application/zip",
        title       = "Questionnaires",
        author      = author,
        description = paste0(zipname, " includes the following files: ", paste(basename(quest_files), collapse = ", "))
      )
      
      res <- create_resource(project_id, resource_body, file_path = zipfile, ME_API_KEY)
      log_resource("Questionnaire", res, idno)
    }
  }
  
  if (!tech_exists && !quest_exists) {
    top_files <- dir_ls(doc_dir, recurse = FALSE, type = "file")
    
    if (length(top_files) > 0) {
      zipname <- paste0("Technical_", idno, ".zip")
      zipfile <- make_zip(zipname, top_files, doc_dir)
      
      resource_body <- list(
        dctype      = "doc/tec",
        dcformat    = "application/zip",
        title       = "Technical Documents",
        author      = author,
        description = paste0(zipname, " includes the following files: ", paste(basename(top_files), collapse = ", "))
      )
      
      res <- create_resource(project_id, resource_body, file_path = zipfile, ME_API_KEY)
      log_resource("Technical documentation", res, idno)
    }
  }
  
  return()
}

## This function identifies additional data files and uploads them as zipped files 
handle_additional_data_resources <- function(project_id, idno, dta_path, ME_API_KEY, row) {
  data_root <- path_dir(path_dir(dta_path))
  data_dir  <- path(data_root, "Additional Data")
  
  if (!dir_exists(data_dir)) {
    message("No Additional Data folder, skipping.")
    return()
  }
  
  author <- if (!is.null(row$producers_name) && !is.na(row$producers_name) && nzchar(trimws(row$producers_name))) {
    row$producers_name
  } else {
    paste("National Statistical Offices of", row$nation_name)
  }
  
  data_files <- dir_ls(data_dir, recurse = TRUE, type = "file")
  
  if (length(data_files) == 0) {
    message("Additional Data folder is empty, skipping.")
    return()
  }
  
  zipname <- paste0("Additional_Data_", idno, ".zip")
  zipfile <- make_zip(zipname, data_files, data_dir)
  
  resource_body <- list(
    dctype      = "dat/oth",
    dcformat    = "application/zip",
    title       = "Additional Data",
    author      = author,
    description = paste0(zipname, " includes the following files: ", paste(basename(data_files), collapse = ", "))
  )
  
  res <- create_resource(project_id, resource_body, file_path = zipfile, ME_API_KEY)
  log_resource("Additional data", res, idno)
  
  return()
}


publish_project <- function(project_id, ME_API_KEY, catalog_connection_id, publish_metadata = TRUE, publish_thumbnail = TRUE, publish_resources = TRUE) {
  
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
    cat("catalog_connection_id: ", catalog_connection_id)
    cat("overwrite: ",             overwrite)
    cat("priority: ",              priority)
    cat("max_attempts: ",          max_attempts)
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

## This function updates the published flag in _ingestion_metadata
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


## This function fetches the project JSON, injects variable labels, and updates the project. It is currently used to publish the harmonized tables CSV.
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
