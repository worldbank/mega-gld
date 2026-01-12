library(jsonlite)
library(httr)
library(sparklyr)
library(dplyr)
library(stringr)
library(fs)
library(zip)

sc <- spark_connect(method = "databricks")

metadata_table <- "prd_csc_mega.sgld48._ingestion_metadata"
json_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/json_to_publish"
path_survey <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/survey_metadata.csv"
survey  <- read.csv(path_survey)
countries_csv <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/countries.csv"
column_names <- c("code", "name") 
countries_names  <- read.csv(countries_csv, header = FALSE, col.names = column_names)


metadata <- tbl(sc, metadata_table) %>%
  collect()

merged_df <- left_join(metadata,survey,by = c("survey", "country"))

merged_df <- merged_df %>%
  mutate(nation_name = countries_names$name[match(country, countries_names$code)])

# api key is currently marina's personal
api_key <- dbutils.secrets.get("GLDKEYVAULT","NADA_API_KEY")

BASE <- "https://metadataeditor.worldbank.org/index.php/api/"


 
# --- helpers
create_dataset <- function(json_data, api_key){
  url <- paste0(BASE, "editor/create/survey")
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    content_type_json(),
    accept_json(),
    body = json_data,
    encode = "json"
  )

  if (httr::status_code(resp) >= 300) {
    err <- httr::content(resp, as = "parsed")
    message("Dataset creation failed", err$message)
    return(NA)
  }
  httr::content(resp, as = "parsed")$id
}

upload_microdata_file <- function(project_id, file_path, api_key){
  url <- paste0(BASE, "data/datafile/", project_id)
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-Key` = api_key),
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
  httr::content(resp, as = "parsed")$result$file_id
}

generate_summary_stats <- function(project_id, file_id, api_key) {
  url <- paste0(
    BASE,
    "data/generate_summary_stats_queue/",
    project_id, "/", file_id
  )
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-Key` = api_key),
    httr::accept_json()
  )

  if (httr::status_code(resp) >= 300) {
    message("Generate summary stats failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
    return(NA)
  }
  httr::content(resp, as = "parsed", simplifyVector = TRUE)$job_id
}

post_summary_stats <- function(project_id, file_id, job_id, api_key, poll_secs = 10, max_wait_secs = 5 * 60) {
  start <- Sys.time()
  repeat {
    resp <- httr::GET(
      paste0(
        BASE,
        "data/summary_stats_queue_status/",
        project_id, "/", file_id, "/", job_id
      ),
      httr::add_headers(`X-API-Key` = api_key),
      httr::accept_json()
    )

    parsed <- tryCatch(httr::content(resp, as = "parsed"), error = function(e) NULL)

    if (!is.null(parsed)) {
      js <- tolower(parsed$job_status)
      vi <- parsed$variables_imported

      if (js %in% c("done", "processing", "failed", "error")) {
        return(list(job_status = js, variables_imported = vi))
      }
    }

    if (difftime(Sys.time(), start, units = "secs") > max_wait_secs) {
      return(list(job_status = "timeout", variables_imported = NA_integer_))
    }

    Sys.sleep(poll_secs)
  }
}

create_resource <- function(project_id, resource_body, file_path, api_key) {
  url <- paste0(BASE, "resources/", project_id)
  body <- c(
    resource_body,
    list(file = httr::upload_file(file_path))
  )
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
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

handle_doc_resources <- function(project_id, idno, dta_path, api_key, row) {
  #zip_out_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/test_zip"
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

  log_resource <- function(kind, res) {
    if (is.na(res)) {
      message(kind, " resource creation failed for ", idno)
    } else if (isTRUE(res)) {
      message(kind, " resource created for ", idno, ", but no id returned")
    } else {
      message(kind, " resource created for ", idno, " (resource_id = ", res, ")")
    }
  }

  make_zip <- function(zipname, files_abs, root_dir) {
    zipfile   <- file.path(tempdir(), zipname)
    rel_files <- fs::path_rel(files_abs, start = root_dir)
    zip::zip(zipfile, files = rel_files, root = root_dir)
    #dest_zip <- path(zip_out_dir, basename(zipfile))
    #file_copy(zipfile, dest_zip, overwrite = TRUE)
    zip_contents <- zip::zip_list(zipfile)$filename
    message("Created ZIP %s with %d files", basename(zipfile), length(zip_contents))
    message("ZIP contents: %s", paste(zip_contents, collapse = ", "))
    zipfile
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

      res <- create_resource(project_id, resource_body, file_path = zipfile, api_key)
      log_resource("Technical documentation", res)
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

      res <- create_resource(project_id, resource_body, file_path = zipfile, api_key)
      log_resource("Questionnaire", res)
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

      res <- create_resource(project_id, resource_body, file_path = zipfile, api_key)
      log_resource("Technical documentation", res)
    }
  }

  return()
}


# --- publication pipe ---

json_files <- list.files(json_dir, pattern="\\.json$", full.names=TRUE)

results <- lapply(json_files, function(jfile){
  message("Processing: ", jfile)
  json_obj <- jsonlite::read_json(jfile)
  fname_json <- basename(jfile)                
  fname_base <- sub("\\.json$","", fname_json)

  # lookup table_name in metadata
  row <- merged_df %>% filter(filename == fname_base)

  if (nrow(row) == 0) {
    warning("No metadata match for ", fname_base)
    return(list(idno=fname_base, status="NO_METADATA"))
  }

  table_name <- row$table_name[1]  
  idno <- paste0("DDI_", row$filename, "_WB")

  # 1 create dataset
  project_id <- create_dataset(json_obj, api_key)
  if (is.na(project_id)) {return(NULL)}
  message("Dataset created, project_id = ", project_id)

  # 2 get and upload file
  dta_path <- row$dta_path[1] 
  file_id <- upload_microdata_file(project_id, dta_path, api_key)
  if (is.na(file_id)) return(NULL)
  message("Dataset uploaded, file_id = ", file_id)

  # 3 queue stats
  job_id <- generate_summary_stats(project_id, file_id, api_key)
  if (is.na(job_id)) return(NULL)
  message("Summary statistics job queued: ", job_id)
  
  # 4 doc resources
  upload_docs <- handle_doc_resources(project_id, idno, dta_path, api_key, row)

  # 5 add do file as ext resources
  if (!is.null(row$do_path) && nzchar(row$do_path)){
    do_path <- row$do_path[1]
    title = paste0("Stata Program for ", row$survey_extended, " ", row$year, " ,Global Labour Database Harmonized Dataset")
    resource_body <- list(
      dctype = "prg",
      dcformat = "text/plain",
      title = title,
      author = "Economic Policy - Growth and Jobs Unit",
      description = "Stata Program for GLD Harmonized Data"
    )

    res_id <- create_resource(project_id, resource_body, file_path = do_path, api_key)
    if (is.na(res_id)) {
      message("Do file resource failed for ", idno)
    } else if (isTRUE(res_id)) {
      message("Do file resource uploaded for ", idno, ", but no id returned")
    } else {
      message("Do file resource uploaded for ", idno, " (resource_id = ", res_id, ")")
    }

  }

  #5 check summary statistics job status
  st <- post_summary_stats(project_id, file_id, job_id, api_key, poll_secs = 10)
  message("Summary stats status: ", st$job_status, " (", st$variables_imported," variableimported)")
 
})


