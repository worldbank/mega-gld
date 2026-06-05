# Databricks notebook source
library(jsonlite)
library(httr)
library(dplyr)
library(stringr)
library(fs)
library(zip)
library(readxl)

# COMMAND ----------

# MAGIC %run "./helpers/config"

# COMMAND ----------

# MAGIC %run "./helpers/publication_pipeline"

# COMMAND ----------

# MAGIC %run "./helpers/json_pipeline"

# COMMAND ----------

# MAGIC %run "./helpers/ai_description"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
  source("helpers/publication_pipeline.r")
  source("helpers/json_pipeline.r")
  source("helpers/ai_description.r")
}

# COMMAND ----------

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  metadata <- tbl(sc, METADATA_TABLE) %>% collect()
  path_survey <- file.path(ROOT_DIR, "survey-metadata.xlsx")
  survey <- read_excel(path_survey)
  
  countries_names <- fetch_countries_names(sc) 
  countries_names <- countries_names %>% rename(nation_name = name)

  merged_df <- left_join(metadata, survey, by = c("survey", "country")) %>%
    left_join(countries_names, by = c("country" = "code"))
  

  json_files <- list.files(JSON_DIR, pattern="\\.json$", full.names=TRUE)
  json_files <- json_files[!grepl("HARMONIZED", json_files)]
  json_files <- sort(json_files)

  ai_token <- get_azure_openai_token()

  results <- lapply(json_files, function(jfile){
    message("-----------------------------")
    message("Processing: ", jfile)
    json_obj <- jsonlite::read_json(jfile)
    fname_json <- basename(jfile)                
    idno <- fname_json %>%
      sub("\\.json$", "", .) 

    # lookup file name in metadata
    row <- merged_df %>% filter(filename == idno)

    if (nrow(row) == 0) {
      warning("No metadata match for ", idno)
      return(list(idno=idno, status="NO_METADATA"))
    }

    if (nrow(row) > 1) {
      warning("Multiple metadata matches for ", idno," (", nrow(row), " rows). Using the first match.")
      row <- row[1, , drop = FALSE]
    }

    dta_path <- row$dta_path[1]
    if (is.na(dta_path) || !nzchar(dta_path) || !file.exists(dta_path)) {
      warning("DTA file not found, skipping: ", idno, " (path: ", dta_path, ")")
      return(list(idno = idno, status = "File not found"))
    }
  
    # 1 create dataset
    created    <- create_project(json_obj, ME_API_KEY)
    if (is.na(created$id)) {return(NULL)}
    project_id <- created$id
    message("Project created, project_id = ", project_id)

    # 2 get and upload file
    file_description <- paste0("Harmonized Dataset of the ", row$year, " ", row$nation_name, " ", row$survey_extended)
    file_id <- upload_microdata_file(project_id, dta_path, ME_API_KEY, description = file_description, overwrite = created$overwrite_used)
    if (is.na(file_id)) return(NULL)
    message("Dataset uploaded to project, file_id = ", file_id)

    # 3 upload external resources
    if (created$overwrite_used) delete_all_resources(project_id, ME_API_KEY)
    author    <- get_author(row)
    doc_root  <- path_dir(path_dir(path_dir(dta_path)))
    doc_dir   <- path(doc_root, "Doc")
    
    # -- technical docs or top-level Doc
    tech_dir  <- path(doc_dir, "Technical")
    tech_exists <- dir_exists(tech_dir)
    tech_source <- if (tech_exists) tech_dir else if (dir_exists(doc_dir)) doc_dir else NULL
    if (!is.null(tech_source)) {
      files <- dir_ls(tech_source, recurse = tech_exists, type = "file")
      lapply(files, function(fp) {

          ai_meta <- tryCatch(get_ai_description_tech(fp, ai_token), error = function(e) { Sys.sleep(5); tryCatch(get_ai_description_tech(fp, ai_token), error = function(e2) NULL) })
          if (is.null(ai_meta) || is.na(ai_meta$title)) {
            ai_meta <- list(
              title       = "Technical Documentation",
              description = paste0("Technical Documentation for the ", row$year, " ", row$nation_name, " ", row$survey_extended)
            )
          }

          resource_body <- list(
            dctype      = "doc/tec",
            dcformat    = mime::guess_type(fp),
            title = paste0(row$nation_name, " (", row$year, ") ", ai_meta$title),
            author      = author,
            filename = basename(fp),
            description = ai_meta$description
          )
          upload_resource(project_id, fp, resource_body,
                          ME_API_KEY, "Technical documentation", idno)
      })
    }

    # -- questionnaires
    quest_dir <- path(doc_dir, "Questionnaires")
    if (dir_exists(quest_dir)) {
      quest_files <- dir_ls(quest_dir, recurse = TRUE, type = "file")
      if (length(quest_files) > 0) {
        lapply(quest_files, function(fp) {

          ai_meta <- tryCatch(get_ai_description_quest(fp, ai_token), error = function(e) { Sys.sleep(5); tryCatch(get_ai_description_quest(fp, ai_token), error = function(e2) NULL) })
          if (is.null(ai_meta) || is.na(ai_meta$title)) {
            ai_meta <- list(
              title       = "Survey Questionnaire",
              description = paste0("Survey Questionnaire for the ", row$year, " ", row$nation_name, " ", row$survey_extended)
            )
          }

          resource_body <- list(
            dctype      = "doc/qst",
            dcformat    = mime::guess_type(fp),
            title = paste0(row$nation_name, " (", row$year, ") ", ai_meta$title),
            author      = author,
            filename    = basename(fp),
            description = ai_meta$description
          )
          upload_resource(project_id, fp, resource_body,
                          ME_API_KEY, "Questionnaire", idno)
        })
      }
    }
    


    # -- additional data
    data_dir <- path(path_dir(path_dir(dta_path)), "Additional Data")
    if (dir_exists(data_dir)) {
      data_files <- dir_ls(data_dir, recurse = TRUE, type = "file")
      if (length(data_files) > 0) {
        lapply(data_files, function(fp) {

          ai_meta <- tryCatch(get_ai_description_data(fp, ai_token), error = function(e) { Sys.sleep(5); tryCatch(get_ai_description_data(fp, ai_token), error = function(e2) NULL) })
          if (is.null(ai_meta) || is.na(ai_meta$title)) {
            ai_meta <- list(
              title       = "Additional Data",
              description = paste0("Additional data for the ", row$year, " ", row$nation_name, " ", row$survey_extended)
            )
          }

          resource_body <- list(
            dctype      = "dat/oth",
            dcformat    = mime::guess_type(fp),
            title       = paste0(row$nation_name, " (", row$year, ") ", ai_meta$title),
            author      = author,
            filename    = basename(fp),
            description = ai_meta$description
          )
          upload_resource(project_id, fp, resource_body,
                          ME_API_KEY, "Additional data", idno)
        })
      }
    }

    # -- do file
    do_path <- row$do_path[1]
    if (!is.na(do_path) && nzchar(do_path)) {
      resource_body <- list(
        dctype      = "prg",
        dcformat    = "text/plain",
        title       = "Stata Program for GLD Harmonized Data",
        author      = "Economic Policy - Growth and Jobs Unit",
        filename = basename(do_path),
        description = paste0("Stata Program for the ", row$year, " ", row$nation_name, " ", row$survey_extended, ", Global Labour Database Harmonized Dataset")
      )
      upload_resource(project_id, do_path, resource_body,
                      ME_API_KEY, "Do file", idno)
    }

    # 4 publish project
    publish <- publish_project(project_id, ME_API_KEY, catalog_connection_id = CATALOG_CONN_ID, classification = row$classification, overwrite_resources = overwrite_resources)
    if (publish$success) {
        cat("Published:", paste0("https://microdatalibqa.worldbank.org/index.php/catalog/study/", idno), "\n")
    } else {
        cat("Publish FAILED for", idno, "\n")
    }

    #5 update _ingestion_metadata table and delete json file if publish succeeded
    if (isTRUE(publish$success)) {
      update_metadata(idno)
      file.remove(jfile)
      message("Deleted json file: ", jfile)
    } else {
      message("Skipping metadata update (publish failed) for: ", idno)
    }
    message("Dataset processing complete")
  
  })
}


