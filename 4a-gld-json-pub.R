library(jsonlite)
library(httr)
library(sparklyr)
library(dplyr)
library(stringr)
library(fs)
library(zip)

sc <- spark_connect(method = "databricks")

metadata_table <- "prd_csc_mega.sgld48._ingestion_metadata"
json_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/jsons_temp/"
path_survey <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/survey_metadata.csv"
survey  <- read.csv(path_survey)
countries_csv <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/countries.csv"
column_names <- c("code", "name") 
countries_names  <- read.csv(countries_csv, header = FALSE, col.names = column_names)


metadata <- tbl(sc, metadata_table) %>%
  collect()


merged_df <- left_join(
  metadata,
  survey,
  by = c("survey", "country")
)

merged_df <- merged_df %>%
  mutate(
    nation_name = countries_names$name[match(country, countries_names$code)]
  )

# api key is not in envvvar yet, this code won't run
api_key <- dbutils.secrets.get("GLDKEYVAULT","NADA_API_KEY")

BASE <- "https://metadataeditor.worldbank.org/index.php/api/"

 
# --- helpers
create_dataset <- function(json_data, api_key){
  url <- paste0(BASE, "datasets")
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    body = json_data,
    encode = "json"
  )
  if (httr::status_code(resp) >= 300) {
    stop("HTTP error: ", httr::status_code(resp))
  }
  httr::content(resp, as="parsed")
}

upload_microdata_file <- function(idno, file_path, api_key){
  url <- paste0(BASE, "datasets/file/", idno)
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    body = list(file = httr::upload_file(file_path))
  )
  if (httr::status_code(resp) >= 300) {
    stop("HTTP error: ", httr::status_code(resp))
  }
  httr::content(resp, as="parsed")
}

attach_to_gld <- function(idno, api_key) {
  url <- paste0(BASE, "datasets/collections")
  
  body <- list(
    study_idno = idno,
    owner_collection = "gld",
    link_collections = list("gld"),
    mode = "update"
  )
  
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    body = body,
    encode = "json"
  )
  if (httr::status_code(resp) >= 300) {
    stop("HTTP error: ", httr::status_code(resp))
  }
  httr::content(resp, as="parsed")
}


publish_dataset <- function(idno, api_key){
  url <- paste0(BASE, "datasets/", idno)
  body <- list(published=1, access_policy="open")

  resp <- httr::PUT(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    body = body,
    encode = "json"
  )
  if (httr::status_code(resp) >= 300) {
    stop("HTTP error: ", httr::status_code(resp))
  }
  httr::content(resp, as="parsed")
}

# plavceholder.this might just be the idno in the json. need to check when the url is whitelisted.
# get_idno <- function(json_obj, filename){
#   if (!is.null(json_obj$idno) && nzchar(json_obj$idno))
#     return(json_obj$idno)
#   sub("\\.json$","", filename)
# }

create_resource <- function(idno, resource_body, api_key){
  url <- paste0(BASE, "datasets/", idno, "/resources")
  
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    body = resource_body,
    encode = "json"
  )
  if (httr::status_code(resp) >= 300) {
    stop("HTTP error: ", httr::status_code(resp))
  }
  httr::content(resp, as="parsed")
}


upload_resource_file <- function(idno, file_path, resourceId=NULL, api_key){
  url <- paste0(BASE, "datasets/", idno, "/files")
  
  body_list <- list(
    file = httr::upload_file(file_path)
  )
  if (!is.null(resourceId)) {
    body_list$resourceId <- resourceId
  }
  
  resp <- httr::POST(
    url,
    httr::add_headers(`X-API-KEY` = api_key),
    body = body_list
  )
  if (httr::status_code(resp) >= 300) {
    stop("HTTP error: ", httr::status_code(resp))
  }
  httr::content(resp, as="parsed")
}

handle_doc_resources <- function(idno, dta_path, api_key,row) {
  
  doc_root <- path_dir(path_dir(dta_path))
  doc_dir <- path(doc_root, "Doc")
  
  if (!dir_exists(doc_dir)) {
    message("No Doc folder, skipping.")
    return()
  }
  
  items <- dir_ls(doc_dir, type="any", recurse=FALSE)
  
  tech_dir <- path(doc_dir, "Technical")
  quest_dir<- path(doc_dir, "Questionnaires")
  
  tech_exists <- dir_exists(tech_dir)
  quest_exists<- dir_exists(quest_dir)
  
  if (tech_exists) {
    tech_files <- dir_ls(tech_dir, recurse=TRUE, type="file")
    
    if (length(tech_files)>0) {
      zipname <- paste0("Technical_", idno, ".zip")
      zipfile <- file.path(tempdir(), zipname)
      zip::zip(zipfile, files=tech_files)
      
      fnames <- paste(basename(tech_files), collapse=", ")
      
      resource_body <- list(
        dctype = "doc/tec",
        dcformat = "application/zip",
        title = "Technical Documents",
        author = if (!is.null(row$producers_name) && !is.na(row$producers_name)&& nzchar(trimws(row$producers_name))) {row$producers_name } else { paste("National Statistical Offices of", row$nation_name)},
        description = paste0(zipname," includes the following files: ", fnames),
        filename = basename(zipfile)  
      )
      
      res <- create_resource(idno, resource_body, api_key)
      upload_resource_file(idno, zipfile, resourceId=res$id, api_key=api_key)
      
      message("Uploaded TECHNICAL docs for ", idno)
    }
  }
  
  if (quest_exists) {
    quest_files <- dir_ls(quest_dir, recurse=TRUE, type="file")
    
    if (length(quest_files)>0) {
      zipname <- paste0("Questionnaires_", idno, ".zip")
      zipfile <- file.path(tempdir(), zipname)
      zip::zip(zipfile, files=quest_files)
      
      fnames <- paste(basename(quest_files), collapse=", ")
      
      resource_body <- list(
        dctype = "doc/qst",
        dcformat = "application/zip",
        title = "Questionnaires",
        author = if (!is.null(row$producers_name) && !is.na(row$producers_name)&& nzchar(trimws(row$producers_name))) {row$producers_name } else { paste("National Statistical Offices of", row$nation_name)},
        description = paste0("Questionnaires_", idno, ".zip includes the following files: ", fnames),
        filename = basename(zipfile)  
      )
      
      res <- create_resource(idno, resource_body, api_key)
      upload_resource_file(idno, zipfile, resourceId=res$id, api_key=api_key)
      
      message("Uploaded QUESTIONNAIRE docs for ", idno)
    }
  }
  

  if (!tech_exists && !quest_exists) {
    top_files <- dir_ls(doc_dir, recurse=FALSE, type="file")
    
    if (length(top_files)>0) {
      zipname <- paste0("Technical_", idno, ".zip")
      zipfile <- file.path(tempdir(), zipname)
      zip::zip(zipfile, files=top_files)
      
      fnames <- paste(basename(top_files), collapse=", ")
      
      resource_body <- list(
        dctype = "doc/tec",
        dcformat = "application/zip",
        title = "Technical Documents",
        author = if (!is.null(row$producers_name) && !is.na(row$producers_name)&& nzchar(trimws(row$producers_name))) {row$producers_name } else { paste("National Statistical Offices of", row$nation_name)},
        description = paste0(zipname," includes the following files: ", fnames),
        filename = basename(zipfile)  
      )
      
      res <- create_resource(idno, resource_body, api_key)
      upload_resource_file(idno, zipfile, resourceId=res$id, api_key=api_key)
      
      message("Uploaded DIRECT DOCs (no subfolders) for ", idno)
    }
  }
    return()
}

# --- publication pipe ---

json_files <- list.files(json_dir, pattern="\\.json$", full.names=TRUE)

results <- lapply(json_files, function(jfile){

  tryCatch({

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
    #idno <- get_idno(json_obj, fname_json) <- this might be the the idno in the json. need to check when the url is whitelisted.
    idno <- row$filename

    # 1 create dataset
    create_dataset(json_obj, api_key)

    # 2 get and upload file
    dta_path <- row$dta_path[1]   
    upload_microdata_file(idno, dta_path, api_key)

    # 3 add do file as ext resources
    if (!is.null(row$do_path) && nzchar(row$do_path)){
      do_path <- row$do_path[1]
      title = paste0("Stata Program for", row$survey_Extended, " ", row$year,  National Occupation and Employment Survey 2020, "Global Labour Database Harmonized Dataset")
      resource_body <- list(
        dctype = "prg",
        dcformat = "text/plain",
        title = title,
        author = "Economic Policy - Growth and Jobs Unit",
        description = "Stata Program for GLD Harmonized Data",
        filename = basename(do_path)
      )
      res <- create_resource(idno, resource_body, api_key)
      resourceId <- res$id 
      upload_resource_file(idno, do_path, resourceId, api_key)
    }

    # 4 doc resources
    handle_doc_resources(idno, dta_path, api_key, row)

    # 5 attach to gld
    attach_to_gld(idno, api_key)

    # 6 publish
    publish_dataset(idno, api_key)

    file.remove(jfile)

    list(idno=idno, status="OK")
  
  }, error = function(e){

    warning("FAILED: ", jfile, " -> ", conditionMessage(e))

    list(idno=fname_base, status="FAILED", error=conditionMessage(e))
  })
})
