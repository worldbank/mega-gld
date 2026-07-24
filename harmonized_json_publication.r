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
    
    csv_dir  <- file.path(CSV_HARMONIZED, paste0(fname_base, "_temp"))
    csv_name <- paste0(fname_base, ".csv")
    zip_path <- file.path(CSV_HARMONIZED, paste0(fname_base, ".csv.zip"))
    
    if (file.exists(zip_path)) {
      message("Found existing file: ", zip_path)
      size_gb <- file.info(zip_path)$size / 1024^3
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
        path   = csv_dir,
        mode   = "overwrite",
        header = TRUE
      )
    
    message("Combining part files into single zipped CSV...")
    local_csv <- file.path("/tmp", csv_name)
    local_zip <- file.path("/tmp", paste0(fname_base, ".csv.zip"))

    first_part <- list.files(csv_dir, pattern = "^part-.*\\.csv$", full.names = TRUE)[1]
    system(sprintf("head -1 '%s' > '%s'", first_part, local_csv))
    system(sprintf("for f in %s/part-*.csv; do tail -n +2 \"$f\"; done >> '%s'", csv_dir, local_csv))
    system(sprintf("zip -j '%s' '%s'", local_zip, local_csv))

    row_count_csv <- as.integer(system(sprintf("unzip -p '%s' | wc -l", local_zip), intern = TRUE)) - 1L
    message("CSV has ", row_count_csv, " rows (excluding header)")

    if (row_count_table != row_count_csv) {
      system(sprintf("rm -f '%s' '%s'", local_csv, local_zip))
      stop("Row count mismatch! Table: ", row_count_table, ", CSV: ", row_count_csv)
    }

    system(sprintf("cp '%s' '%s'", local_zip, zip_path))
    system(sprintf("rm -f '%s' '%s'", local_csv, local_zip))

    size_gb <- file.info(zip_path)$size / 1024^3
    message(sprintf("File size: %.2f GB", size_gb))
    
    system(sprintf("rm -rf '%s'", csv_dir))
    
    message("ZIP ready: ", zip_path)
  })
}

# COMMAND ----------

if (is_databricks()) {
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
    
    csv_path <- file.path(CSV_HARMONIZED, paste0(fname_base, ".csv.zip"))
    
    if (!file.exists(csv_path)) {
      message("ERROR: Compressed file not found: ", csv_path)
      return(NULL)
    }

    # 1 create project
    is_ouo         <- grepl("HARMONIZED_OUO", fname_base)
    classification <- if (is_ouo) "Official Use" else "Confidential"

    project_id <- create_project(json_obj, ME_API_KEY)
    if (is.na(project_id)) {
      message("ERROR: Could not create project")
      return(NULL)
    } else {
      message("Dataset created, project_id = ", project_id)
      publish <- publish_project(project_id, ME_API_KEY, catalog_connection_id = CATALOG_CONN_ID, classification = classification)
        if (publish$success) {
            message("Published:", paste0("https://microdatalibqa.worldbank.org/index.php/catalog/study/", idno), "\n")
            print(project_id)
            print(idno)
        } else {
            message("Publish FAILED for ", idno)
            print(project_id)
            print(idno)
        }
    }


    # 2 create catalog table, upload zipped CSV, and import
    message("Publishing catalog table...")

    table_result <- publish_table_file(
      db_id       = "GLD",
      table_id    = idno,
      file_path   = csv_path,
      title       = json_obj$study_desc$title_statement$title,
      description = json_obj$study_desc$title_statement$title,
      NADA_API_KEY   = NADA_API_KEY
    )
    if (identical(table_result, NA)) {
      message("ERROR: Table publish failed")
      return(NULL)
    }

    # 3 attach table to study
    message("Attaching table to study...")
    attached <- attach_table_to_study(
      db_id      = "GLD",
      table_id   = idno,
      idno       = idno,
      NADA_API_KEY   = NADA_API_KEY
    )
    if (is.na(attached) || !isTRUE(attached)) {
      message("ERROR: Table attach failed")
      return(NULL)
    }
    message("Table attached to study: ", idno)


    # 4 update ingestion metadata and cleanup
    if (isTRUE(publish$success)) {
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

      record_published_version(
        sc            = sc,
        filename      = idno,
        table_name    = table_suffix,
        v_version     = published_version,
        table_version = current_table_version,
        version_notes = json_obj$study_desc$version_statement$version_notes
      )

      # Delete files after successful publish
      file.remove(jfile)
      message("Deleted json file: ", jfile)
    } else {
      message("Skipping metadata update (publish failed) for: ", fname_base)
    }
  
  })
}
