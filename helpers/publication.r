# Databricks notebook source
library(httr)
library(fs)
library(zip)
library(sparklyr)

# COMMAND ----------

# MAGIC %run "./config"

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

## This function will need to be rewritten once API is fixed
## catalog_connection_id needs to be updated before productionizing
publish_project <- function(project_id, ME_API_KEY, catalog_connection_id, repositoryid, access_policy = "licensed",overwrite = "yes",published = 0) {
  
  call_get <- function(path) {
    url <- paste0(METADATA_API_BASE, path, "/", project_id)
    resp <- httr::GET(
      url,
      httr::add_headers(`X-API-KEY` = ME_API_KEY)
    )
    parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
    list(url = url, status_code = httr::status_code(resp), response = parsed)
  }
  
  call_post <- function(body) {
    url <- paste0(METADATA_API_BASE, "publish/", project_id, "/", catalog_connection_id)
    resp <- httr::POST(
      url,
      httr::add_headers(`X-API-KEY` = ME_API_KEY),
      body = body,
      encode = "json"
    )
    parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
    list(url = url, status_code = httr::status_code(resp), response = parsed)
  }
  
  out <- list()
  
  out$generate_json <- call_get("editor/generate_json")
  out$generate_ddi  <- call_get("editor/generate_ddi")
  out$write_json    <- call_get("resources/write_json")
  out$write_rdf     <- call_get("resources/write_rdf")
  out$generate_zip  <- call_get("packager/generate_zip")
  
  out$publish <- call_post(list(
    repositoryid  = repositoryid,
    access_policy = access_policy,
    overwrite     = overwrite,
    published     = published
  ))
  
  out$success <- isTRUE(out$publish$status_code < 300)
  
  if (!out$success) {
    msg <- out$publish$response$message
    if (is.null(msg) || is.na(msg) || !nzchar(msg)) msg <- "Unknown error"
    message("Dataset publish failed ", msg)
  }
  
  out
}


## This function updates the published flag in _ingestion_metadata
update_metadata <- function(fname_base) {
  sparklyr::spark_sql(
    sc,
    paste0(
      "UPDATE ", METADATA_TABLE, "
       SET published = TRUE
       WHERE fname_base = '", fname_base, "'"
    )
  )
  message("Updated metadata for: ", fname_base)
}
