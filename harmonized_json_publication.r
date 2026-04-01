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

# MAGIC %run "./helpers/stacking_functions"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

if (!exists("create_dataset")) {
  source("helpers/publication_pipeline.r")
  source("helpers/stacking_functions.r")
}

# COMMAND ----------

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  ME_API_KEY <- ME_API_KEY

  json_files <- list.files(JSON_DIR, pattern="HARMONIZED.*\\.json$", full.names=TRUE)

  results <- lapply(json_files, function(jfile){
    message("-----------------------------")
    message("Processing: ", jfile)
    json_obj <- jsonlite::read_json(jfile)

    fname_json <- basename(jfile)

    idno <- fname_json %>%
      sub("\\.json$", "", .) 

    fname_base <- idno %>%
      sub("^DDI_", "", .) %>%
      sub("_WB$", "", .)
    

    # 1 create dataset
    project_id <- create_dataset(json_obj, ME_API_KEY)
    if (is.na(project_id)) {return(NULL)}
    message("Dataset created, project_id = ", project_id)

    # 2 create and upload file
    table_suffix <- tolower(sub("_V[0-9]+$", "", fname_base))  
    table_suffix <- sub("^GLD_", "", table_suffix)
    table_name <- paste0(TARGET_SCHEMA,".", table_suffix)    
    message("Exporting table ", table_name, " to CSV...")
    
    df <- tbl(sc, table_name) %>% collect()
    csv_path <- file.path(CSV_HARMONIZED, paste0(fname_base, ".csv"))
    write.csv(df, csv_path, row.names = FALSE)
    
    message("CSV created at: ", csv_path)
    file_id <- upload_microdata_file(project_id, csv_path, ME_API_KEY)
    if (is.na(file_id)) return(NULL)
    message("Dataset uploaded, file_id = ", file_id)
    

    # # 3 publish project
    publish <- publish_project(project_id, ME_API_KEY, catalog_connection_id = CATALOG_CONN_ID)
    if (publish$success) {
        cat("Published:", paste0("https://microdatalibqa.worldbank.org/index.php/catalog/", project_id), "\n")
    } else {
        cat("Publish FAILED for", idno, "\n")
    }

    # 4 update _ingestion_metadata table for published harmonized data
    if (isTRUE(publish$success)) {
      is_ouo <- grepl("HARMONIZED_OUO", fname_base)
      published_version <- as.integer(sub(".*_V([0-9]+)$", "\\1", fname_base))
      published_column <- if (is_ouo) {"stacked_ouo_published"} else {"stacked_all_published"}
      version_column <- if (is_ouo) {"stacked_ouo_table_version"} else {"stacked_all_table_version"}
      current_table_version <- get_delta_table_version(table_name, sc)
      
      message(sprintf("Publishing version: %d, Current table version: %d", published_version, current_table_version))
      
      query <- paste0(
        "SELECT country, year, survey, quarter, M_version, A_version, table_name
        FROM ", METADATA_TABLE, "
        WHERE ", version_column, " IS NOT NULL
        AND ", version_column, " <= ", current_table_version, "
        AND (", published_column, " IS NULL OR ", published_column, " = 0)"
      )
      
      metadata_df <- DBI::dbGetQuery(sc, query)
      
      
      for (i in seq_len(nrow(metadata_df))) {
        row <- metadata_df[i, ]
        
        sparklyr::spark_sql(
          sc,
          paste0(
            "UPDATE ", METADATA_TABLE, "
            SET ", published_column, " = ", published_version, "
            WHERE country = '", row$country, "'
            AND year = '", row$year, "'
            AND survey = '", row$survey, "'
            AND quarter = '", row$quarter, "'
            AND M_version = ", row$M_version, "
            AND A_version = ", row$A_version, "
            AND table_name = '", row$table_name, "'"
          )
        )
      }
      
      message("Updated metadata: marked ", nrow(metadata_df), " records as published with version ", published_version)
      
      # Delete json file after successful publish
      file.remove(jfile)
      message("Deleted json file: ", jfile)
    } else {
      message("Skipping metadata update (publish failed) for: ", fname_base)
    }
  
  })
}


