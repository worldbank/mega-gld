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
  
    # 1 create dataset
    project_id <- create_project(json_obj, ME_API_KEY)
    if (is.na(project_id)) {return(NULL)}
    message("Project created, project_id = ", project_id)

    # 2 get and upload file
    dta_path <- row$dta_path[1] 
    file_id <- upload_microdata_file(project_id, dta_path, ME_API_KEY)
    if (is.na(file_id)) return(NULL)
    message("Dataset uploaded to project, file_id = ", file_id)
    
    # 3 upload external resources
    author    <- get_author(row)
    doc_root  <- path_dir(path_dir(path_dir(dta_path)))
    doc_dir   <- path(doc_root, "Doc")
    
    # technical docs or top-level Doc: upload individually
    tech_dir  <- path(doc_dir, "Technical")
    tech_exists <- dir_exists(tech_dir)
    tech_source <- if (tech_exists) tech_dir else if (dir_exists(doc_dir)) doc_dir else NULL
    if (!is.null(tech_source)) {
      files <- dir_ls(tech_source, recurse = tech_exists, type = "file")
      lapply(files, function(fp) {

          description <- tryCatch(
              get_ai_description(fp, ai_token),
              error = function(e) basename(fp))

          resource_body <- list(
            dctype      = "doc/tec",
            dcformat    = mime::guess_type(fp),
            title       = description,
            author      = author,
            filename = basename(fp),
            description = description
          )
          upload_resource(project_id, fp, resource_body,
                          ME_API_KEY, "Technical documentation", idno)
      })
    }

    # questionnaires: zipped
    quest_dir <- path(doc_dir, "Questionnaires")
    if (dir_exists(quest_dir)) {
      quest_files <- dir_ls(quest_dir, recurse = TRUE, type = "file")
      if (length(quest_files) > 0) {
        zipname <- paste0("Questionnaires_", idno, ".zip")
        zipfile <- make_zip(zipname, quest_files, quest_dir)
        resource_body <- list(
          dctype      = "doc/qst",
          dcformat    = "application/zip",
          title       = "Survey Questionnaire",
          author      = author,
          filename    = zipname,
          description = paste0(zipname, " includes the following files: ",
                               paste(basename(quest_files), collapse = ", "))
        )
        upload_resource(project_id, zipfile, resource_body,
                        ME_API_KEY, "Questionnaire", idno)
      }
    }

    # additional data: zipped
    data_dir <- path(path_dir(path_dir(dta_path)), "Additional Data")
    if (dir_exists(data_dir)) {
      data_files <- dir_ls(data_dir, recurse = TRUE, type = "file")
      if (length(data_files) > 0) {
        zipname <- paste0("Additional_Data_", idno, ".zip")
        zipfile <- make_zip(zipname, data_files, data_dir)
        resource_body <- list(
          dctype      = "dat/oth",
          dcformat    = "application/zip",
          title       = "Additional Data",
          author      = author,
          filename    = zipname,
          description = paste0(zipname, " includes the following files: ",
                               paste(basename(data_files), collapse = ", "))
        )
        upload_resource(project_id, zipfile, resource_body,
                        ME_API_KEY, "Additional data", idno)
      }
    }

    # do file
    do_path <- row$do_path[1]
    if (!is.na(do_path) && nzchar(do_path)) {
      resource_body <- list(
        dctype      = "prg",
        dcformat    = "text/plain",
        title       = paste0("Stata Program for ", row$survey_extended, " ", row$year,
                             ", Global Labour Database Harmonized Dataset"),
        author      = "Economic Policy - Growth and Jobs Unit",
        filename = basename(do_path),
        description = "Stata Program for GLD Harmonized Data"
      )
      upload_resource(project_id, do_path, resource_body,
                      ME_API_KEY, "Do file", idno)
    }

    # 4 publish project
    publish <- publish_project(project_id, ME_API_KEY, catalog_connection_id = CATALOG_CONN_ID)
    if (publish$success) {
        cat("Published:", paste0("https://microdatalibqa.worldbank.org/index.php/catalog/study/", idno), "\n")
    } else {
        cat("Publish FAILED for", idno, "\n")
    }

    # 5 update _ingestion_metadata table and delete json file if publish succeeded
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


