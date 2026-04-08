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
# MAGIC

# COMMAND ----------

# MAGIC %run "./helpers/publication_pipeline"

# COMMAND ----------

# MAGIC %run "./helpers/json_pipeline"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

if (!exists("create_dataset")) {
  source("helpers/publication_pipeline.r")
}

# COMMAND ----------

if (is_databricks()) {
  sc <- spark_connect(method = "databricks")

  metadata <- tbl(sc, METADATA_TABLE) %>% collect()
  path_survey <- file.path(ROOT_DIR, "survey-metadata.xlsx")
  survey <- read_excel(path_survey)
  
  countries_names <- fetch_countries_names(sc)

  merged_df <- left_join(metadata, survey, by = c("survey", "country")) %>%
    left_join(countries_names, by = c("country" = "code"))
  

  json_files <- list.files(JSON_DIR, pattern="\\.json$", full.names=TRUE)
  json_files <- json_files[!grepl("HARMONIZED", json_files)]

  results <- lapply(json_files, function(jfile){
    message("-----------------------------")
    message("Processing: ", jfile)
    json_obj <- jsonlite::read_json(jfile)
    fname_json <- basename(jfile)                
    idno <- fname_json %>%
      sub("\\.json$", "", .) 
    print(idno)

    # lookup file name in metadata
    row <- merged_df %>% filter(filename == idno)
    print(row)
    if (nrow(row) == 0) {
      warning("No metadata match for ", idno)
      return(list(idno=idno, status="NO_METADATA"))
    }

    if (nrow(row) > 1) {
      warning("Multiple metadata matches for ", idno," (", nrow(row), " rows). Using the first match.")
      row <- row[1, , drop = FALSE]
    }
  
    # 1 create dataset
    project_id <- create_dataset(json_obj, ME_API_KEY)
    if (is.na(project_id)) {return(NULL)}
    message("Dataset created, project_id = ", project_id)

    # 2 get and upload file
    dta_path <- row$dta_path[1] 
    file_id <- upload_microdata_file(project_id, dta_path, ME_API_KEY)
    if (is.na(file_id)) return(NULL)
    message("Dataset uploaded, file_id = ", file_id)
    
    # 3 upload doc resources
    upload_docs <- handle_doc_resources(project_id, idno, dta_path, ME_API_KEY, row)

    # 4 add do file as ext resources
    do_path <- row$do_path[1]
    if (!is.na(do_path) && nzchar(do_path)){
  
      title = paste0("Stata Program for ", row$survey_extended, " ", row$year, " ,Global Labour Database Harmonized Dataset")
      resource_body <- list(
        dctype = "prg",
        dcformat = "text/plain",
        title = title,
        author = "Economic Policy - Growth and Jobs Unit",
        description = "Stata Program for GLD Harmonized Data"
      )

      res_id <- create_resource(project_id, resource_body, file_path = do_path, ME_API_KEY)
      log_resource("Do file", res_id, idno)
    }

    # 5 upload additional data
    upload_data <- handle_additional_data_resources(project_id, idno, dta_path, ME_API_KEY, row)

    # 6 publish project
    publish <- publish_project(project_id, ME_API_KEY, catalog_connection_id = CATALOG_CONN_ID)
    if (publish$success) {
        cat("Published:", paste0("https://microdatalibqa.worldbank.org/index.php/catalog/study/", idno), "\n")
    } else {
        cat("Publish FAILED for", idno, "\n")
    }

    # 7 update _ingestion_metadata table and delete json file if publish succeeded
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


