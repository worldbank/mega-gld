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

if (!exists("create_project")) {
  source("helpers/publication_pipeline.r")
  source("helpers/stacking_functions.r")
}

# COMMAND ----------

if (is_databricks()) {
  library(sparklyr)
  sc <- spark_connect(method = "databricks")

  json_files <- list.files(JSON_DIR, pattern="HARMONIZED.*\\.json$", full.names=TRUE)

  lapply(json_files, function(jfile){
    message("-----------------------------")
    message("Processing: ", jfile)
    
    fname_json <- basename(jfile)
    idno <- fname_json %>% sub("\\.json$", "", .) 
    fname_base <- idno %>% sub("^DDI_", "", .) %>% sub("_WB$", "", .)
    
    table_suffix <- tolower(sub("_V[0-9]+$", "", fname_base))  
    table_suffix <- sub("^GLD_", "", table_suffix)
    table_name <- paste0(TARGET_SCHEMA,".", table_suffix)    
    
    csv_dir <- file.path(CSV_HARMONIZED, paste0(fname_base, "_temp"))
    csv_path <- file.path(CSV_HARMONIZED, paste0(fname_base, ".csv"))
    
    if (file.exists(csv_path)) {
      message("Found existing file: ", csv_path)
      size_gb <- file.info(csv_path)$size / 1024^3
      message(sprintf("File size: %.2f GB", size_gb))
      return(NULL)
    }
    
    message("Exporting table ", table_name, " to CSV...")
    
    row_count_table <- tbl(sc, table_name) %>% 
      count() %>% 
      collect() %>% 
      pull(n)
    message("Table has ", row_count_table, " rows")
    
    tbl(sc, table_name) %>%
      sparklyr::spark_write_csv(
        path = csv_dir,
        mode = "overwrite",
        header = TRUE
      )
    
    message("Combining part files into single CSV...")
    system(sprintf("head -1 %s/part-00000*.csv > %s", csv_dir, csv_path))
    system(sprintf("tail -n +2 -q %s/part-*.csv >> %s", csv_dir, csv_path))
    
    row_count_csv <- as.integer(system(sprintf("wc -l < %s", csv_path), intern = TRUE)) - 1
    message("CSV has ", row_count_csv, " rows (excluding header)")
    
    if (row_count_table != row_count_csv) {
      stop("Row count mismatch! Table: ", row_count_table, ", CSV: ", row_count_csv)
    }
    
    size_gb <- file.info(csv_path)$size / 1024^3
    message(sprintf("File size: %.2f GB", size_gb))
    
    system(sprintf("rm -rf %s", csv_dir))
    
    message("CSV ready: ", csv_path)
  })
}

# COMMAND ----------

if (is_databricks()) {
  # PHASE 2: Upload and publish all tables
  results <- lapply(json_files, function(jfile){
    message("-----------------------------")
    message("Processing: ", jfile)
    json_obj <- jsonlite::read_json(jfile)

    fname_json <- basename(jfile)
    idno <- fname_json %>% sub("\\.json$", "", .) 
    fname_base <- idno %>% sub("^DDI_", "", .) %>% sub("_WB$", "", .)
    
    table_suffix <- tolower(sub("_V[0-9]+$", "", fname_base))  
    table_suffix <- sub("^GLD_", "", table_suffix)
    table_name <- paste0(TARGET_SCHEMA,".", table_suffix)    
    
    csv_path <- file.path(CSV_HARMONIZED, paste0(fname_base, ".csv"))
    
    if (!file.exists(csv_path)) {
      message("ERROR: Compressed file not found: ", csv_path)
      return(NULL)
    }

    # 1 create project
    project_id <- create_project(json_obj, ME_API_KEY)
    if (is.na(project_id)) {
      message("ERROR: Dataset creation failed")
      return(NULL)
    }
    message("Dataset created, project_id = ", project_id)

    # 2 upload microdata (chunked)
    message("Uploading microdata...")
    file_id <- upload_microdata_file(project_id, csv_path, ME_API_KEY)
    if (is.na(file_id)) {
      message("ERROR: Microdata upload failed")
      return(NULL)
    }
    message("Dataset uploaded, file_id = ", file_id)
    
    # 3 add variable labels
    message("Adding variable labels...")
    add_labels <- update_project_with_variables(project_id, table_name, sc, ME_API_KEY)
    if (is.na(add_labels)) {
      message("ERROR: Variable label update failed")
      dbutils.notebook.exit("Variable label update FAILED")
    }

    # 4 publish project
    publish <- publish_project(project_id, ME_API_KEY, catalog_connection_id = CATALOG_CONN_ID)
    if (publish$success) {
        message("Published: https://microdatalibqa.worldbank.org/index.php/catalog/", project_id)
    } else {
        message("Publish FAILED for ", idno)
    }

    # # 5 update ingestion metadata and cleanup
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
      message("Found ", nrow(metadata_df), " records to mark as published")
      
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
      
      # Delete files after successful publish
      file.remove(jfile)
      message("Deleted json file: ", jfile)
    } else {
      message("Skipping metadata update (publish failed) for: ", fname_base)
    }
  
  })
}
