# Databricks notebook source
# Core data processing functions for GLD stacking pipeline
library(dplyr)
library(sparklyr)

OFFICIAL_CLASS <- "Official Use"

#' Validate that the metadata table meets all expected data quality requirements
#' before entering the stacking pipeline. Stops with an informative error
#' if any check fails.
#'
#' @param df A data.frame (or tibble) to validate.
#' @param caller Name of the calling context, used in error messages.
validate_metadata_inputs <- function(df, caller = "unknown") {
  errors <- character()

  # quarter must be a non-null string ("NA" for annual surveys)
  if ("quarter" %in% names(df) && any(is.na(df$quarter))) {
    errors <- c(errors, "column 'quarter' contains NA values — use the string 'NA' for annual surveys")
  }

  if (length(errors) > 0) {
    stop(
      sprintf("%s: metadata validation failed:\n - %s", caller, paste(errors, collapse = "\n - ")),
      call. = FALSE
    )
  }
}

#' Identify tables that need to be updated based on version changes
#'
#' @param metadata_df DataFrame containing ingestion metadata
#' @return DataFrame with tables that have newer versions
identify_changes <- function(metadata_df) {
  # Detect tables where either stacked version is NULL (not yet in harmonized table)
  # AND table_version is 1 (only process first version)
  change_keys <- metadata_df %>%
    filter(
      stacking == 1 & (
        # Case 1: Needs to go into the ALL table
        is.na(stacked_all_table_version) | 
        
        # Case 2: It's an "Official Use" survey and it's missing from the OUO table
        (classification == !!OFFICIAL_CLASS & is.na(stacked_ouo_table_version))
      )
    ) %>%
    select(
      table_name,
      classification,
      countrycode = country,
      year,
      quarter,
      survname = survey,
      table_version,
      stacked_all_table_version,
      stacked_ouo_table_version
    ) %>%
    # EXPLICIT CASTING TO MATCH TARGET TABLE
    mutate(
      year = as.integer(year),
      countrycode = as.character(countrycode),
      survname = as.character(survname),
      quarter = as.character(quarter)
    ) %>%
    distinct()
  
  return(change_keys)
}


#' Build a list of updates from the change keys DataFrame
#'
#' @param change_keys_df DataFrame with tables that need updates
#' @return List of lists, each containing: table_name, classification, country, year, quarter, survey
build_update_list <- function(change_keys_df) {
  # Collect the data
  change_data <- change_keys_df %>% collect()
  
  update_list <- list()
  
  for (i in seq_len(nrow(change_data))) {
    row <- change_data[i, ]
    
    table_name <- row$table_name
    classification <- row$classification
    country <- row$countrycode
    year <- row$year
    survname <- row$survname
    quarter <- row$quarter
    table_version <- row$table_version
    
    # Add to update list
    update_list[[i]] <- list(
      table_name = table_name,
      classification = classification,
      country = country,
      year = year,
      survname = survname,
      quarter = quarter
    )
    
    # Log the action
    if (is.na(row$stacked_all_table_version)) {
      message(sprintf(
        "ACTION: Adding BRAND NEW data for %s %s %s %s of the latest version %s",
        country, year, quarter, survname, table_version
      ))
    } else {
      message(sprintf(
        "ACTION: UPDATING existing data for %s %s %s %s with the latest version %s (Newer version detected)",
        country, year, quarter, survname, table_version
      ))
    }
  }
  
  return(update_list)
}


#' Align a source DataFrame to the standard schema
#'
#' @param src_df Source DataFrame to align
#' @param schema Target schema (named list with column names and types)
#' @param country_val Country code value to add
#' @param survey_val Survey name value to add
#' @return List with two elements: aligned_df (DataFrame) and extra_cols (character vector)
align_dataframe_to_schema <- function(src_df, schema, country_val, survey_val, quarter_val) {
  
  # Extract expected columns from schema
  expected_cols <- names(schema)
  
  # Get source columns
  src_cols <- colnames(src_df)
  
  # Build mutate expressions for expected columns
  mutate_exprs <- list()
  
  for (col_name in expected_cols) {
    target_type <- schema[[col_name]]
    
    if (col_name == "countrycode" && !is.null(country_val)) {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", country_val, "' AS ", target_type, ")"))
    } else if (col_name == "survname" && !is.null(survey_val)) {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", survey_val, "' AS ", target_type, ")"))
    } else if (col_name == "quarter" && !is.null(quarter_val)) {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", quarter_val, "' AS ", target_type, ")"))
    }  else if (col_name %in% src_cols) {
      # Column exists in source, cast it to schema type
      mutate_exprs[[col_name]] <- sql(paste0("CAST(", col_name, " AS ", target_type, ")"))
    } else {
      # Column is missing in source, fill with NULL
      mutate_exprs[[col_name]] <- sql(paste0("CAST(NULL AS ", target_type, ")"))
    }
  }
  
  # Identify dynamic columns (subnational and GAUL)
  dynamic_cols <- src_cols[sapply(src_cols, is_dynamic_column)]
  dynamic_cols <- setdiff(dynamic_cols, expected_cols)
  
  # Add dynamic columns to mutate expressions
  for (dc in dynamic_cols) {
    mutate_exprs[[dc]] <- expr(as.character(!!sym(dc)))
  }
  
  # Final column list
  final_column_names <- c(expected_cols, dynamic_cols)
  
  # Identify extra columns that will be ignored
  extra_cols <- setdiff(src_cols, final_column_names)
  
  # Apply transformation
  aligned_df <- src_df %>%
    mutate(!!!mutate_exprs) %>%
    select(any_of(final_column_names))
  
  return(list(
    aligned_df = aligned_df,
    extra_cols = extra_cols
  ))
}


#' Update metadata with new stacked versions
#'
#' @param metadata_df Original metadata DataFrame
#' @param change_keys_df DataFrame with tables that need updates (includes table_version)
#' @param harmonized_all_table Full table name for harmonized ALL table
#' @param harmonized_ouo_table Full table name for harmonized OUO table
#' @param sc Spark connection
#' @return Updated metadata DataFrame
update_metadata_versions <- function(metadata_df, change_keys_df,
                                    harmonized_all_table, harmonized_ouo_table, sc) {
  new_all_version <- as.integer(get_delta_table_version(harmonized_all_table, sc))
  new_ouo_version <- as.integer(get_delta_table_version(harmonized_ouo_table, sc))
  
  updates_df <- change_keys_df %>%
    select(
      country = countrycode,
      year,
      survey = survname,
      quarter
    ) %>%
    mutate(to_update = 1L)
  
  metadata_final <- metadata_df %>%
    left_join(updates_df, by = c("country", "year", "survey", "quarter")) %>%
    mutate(
      stacked_all_table_version = as.integer(if_else(
        !is.na(to_update),
        new_all_version,
        as.integer(stacked_all_table_version)
      )),
      stacked_ouo_table_version = as.integer(if_else(
        (!is.na(to_update) & classification == "Official Use"),
        new_ouo_version,
        as.integer(stacked_ouo_table_version)
      ))
    ) %>%
    select(-to_update)
  
  return(metadata_final)
}


#' Get the latest version number of a Delta table
#'
#' @param table_name Full table name (e.g., "catalog.schema.table")
#' @param sc Spark connection
#' @return Integer representing the latest version number
get_delta_table_version <- function(table_name, sc) {
  # Split the name by the dots and quote each part individually
  parts <- unlist(strsplit(table_name, "\\."))
  quoted_parts <- paste0("`", parts, "`", collapse = ".")
  
  history_query <- sprintf("DESCRIBE HISTORY %s", quoted_parts)
  
  # Execute
  history <- DBI::dbGetQuery(sc, history_query)
  
  if (nrow(history) == 0) {
    stop(sprintf("No history found for table %s", table_name))
  }
  
  return(max(history$version))
}


#' Validate that changes were detected
#'
#' @param change_keys_df DataFrame with detected changes
#' @return Number of changes detected
validate_change_detection <- function(change_keys_df) {
  num_changes <- change_keys_df %>% count() %>% collect() %>% pull(n)
  
  message(sprintf("Found %d table(s) that need updating", num_changes))
  
  if (num_changes == 0) {
    dbutils.notebook.exit("All tables are up-to-date. No stacking needed.")
  }

    duplicate_check <- change_keys_df %>%
      group_by(countrycode, year, survname, quarter) %>%
      count() %>%
      filter(n > 1) %>%
      collect()
  
    if (nrow(duplicate_check) > 0) {
      message("ERROR: Found duplicate keys in change_keys!")
      message("The following country/year/survname combinations have multiple rows:")
      print(duplicate_check)
      stop("Duplicate keys detected. Each country/year/survname/quarter should appear only once.")
    }
    
    message("✓ Validated: All keys are unique (one row per country/year/survname/quarter)")
    return(TRUE)
  }



#' Validate that records were removed correctly via anti-join
#'
#' @param original_df Original DataFrame before cleaning
#' @param cleaned_df DataFrame after anti-join
#' @param change_keys_df DataFrame with keys to remove
#' @param table_name Name of table for logging
#' @return Number of records removed
validate_record_removal <- function(original_df, cleaned_df, change_keys_df, table_name = "table") {
  # Count records before and after
  original_count <- original_df %>% count() %>% collect() %>% pull(n)
  cleaned_count <- cleaned_df %>% count() %>% collect() %>% pull(n)
  removed_count <- original_count - cleaned_count
  
  message(sprintf("✓ Removed %d record(s) from %s", removed_count, table_name))
  
  # Verify no overlapping records remain
  duplicate_check <- cleaned_df %>%
    inner_join(
      change_keys_df %>% select(countrycode, year, survname, quarter),
      by = c("countrycode", "year", "survname", "quarter")
    ) %>%
    count() %>%
    collect() %>%
    pull(n)
  
  if (duplicate_check > 0) {
    stop(sprintf(
      "ERROR: Found %d record(s) in %s that should have been removed!",
      duplicate_check,
      table_name
    ))
  }
  
  return(removed_count)
}


#' Validate that expected number of tables were processed
#'
#' @param processed_count Actual number of tables processed
#' @param update_list List of updates
#' @return TRUE if validation passes
validate_processing_count <- function(processed_count, update_list) {
  expected_processed <- length(update_list)
  
  if (processed_count != expected_processed) {
    message(sprintf(
      "WARNING: Expected to process %d tables, but processed %d",
      expected_processed,
      processed_count
    ))
    return(FALSE)
  } else {
    message(sprintf("✓ Processed %d table(s) as expected", processed_count))
    return(TRUE)
  }
}


#' Validate that metadata was synchronized correctly
#'
#' @param metadata_table_name Full table name to read metadata from
#' @param change_keys_df DataFrame with expected changes
#' @param harmonized_all_table Full table name for harmonized ALL table
#' @param harmonized_ouo_table Full table name for harmonized OUO table
#' @param sc Spark connection
#' @return TRUE if validation passes, stops with error otherwise
validate_metadata_sync <- function(metadata_table_name, change_keys_df, 
                                  harmonized_all_table, harmonized_ouo_table, sc) {
  # Get the actual Delta table versions
  all_current_version <- get_delta_table_version(harmonized_all_table, sc)
  ouo_current_version <- get_delta_table_version(harmonized_ouo_table, sc)
  
  # Read back the updated metadata
  metadata_check <- tbl(sc, metadata_table_name)
  
  # Collect change_keys first to ensure columns are accessible
  change_keys_collected <- change_keys_df %>%
    select(countrycode, year, survname, quarter) %>%
    collect()
  
  # Join with metadata to verify updates
  validation <- metadata_check %>%
    select(country, year, survey, quarter, stacked_all_table_version, stacked_ouo_table_version) %>%
    mutate(year = as.integer(year)) %>%
    collect() %>%
    inner_join(
      change_keys_collected,
      by = c("country" = "countrycode", "year" = "year", "survey" = "survname", "quarter"="quarter")
    )
  
  # Check that all stacked versions match the actual Delta table versions
  sync_errors <- validation %>%
    filter(
      stacked_all_table_version != all_current_version | 
      stacked_ouo_table_version != ouo_current_version
    )
  
  if (nrow(sync_errors) > 0) {
    message("ERROR: Metadata sync validation failed!")
    message(sprintf("Expected versions - ALL: %d, OUO: %d", all_current_version, ouo_current_version))
    message("The following tables have mismatched versions:")
    print(sync_errors %>% select(country, year, survey, quarter,
                                  stacked_all_table_version, stacked_ouo_table_version))
    stop("Metadata synchronization failed. Please investigate.")
  }
  
  message(sprintf("✓ Metadata sync validated: %d table(s) updated successfully", nrow(validation)))
  message(sprintf("✓ Actual Delta versions - ALL: %d, OUO: %d", all_current_version, ouo_current_version))

  return(TRUE)
}


#' Materialize new DataFrames in batches to temp tables, then union with
#' cleaned existing data for a single atomic overwrite of the production table.
#'
#' @param new_dfs      List of Spark DataFrames to append.
#' @param cleaned_df   Spark DataFrame of existing records (already anti-joined).
#' @param target_table Full production table name.
#' @param sc           Spark connection.
#' @param batch_size   Number of DataFrames per batch.
batched_write_table <- function(new_dfs, cleaned_df, target_table, sc,
                                batch_size = BATCH_SIZE) {
  if (length(new_dfs) == 0) {
    message(sprintf("No new data to write to %s — skipping.", target_table))
    return(invisible(NULL))
  }

  temp_names <- character()
  batches <- split(seq_along(new_dfs), ceiling(seq_along(new_dfs) / batch_size))

  for (b in seq_along(batches)) {
    idx <- batches[[b]]
    tmp_name <- sprintf("tmp_batch_%d", b)
    temp_names <- c(temp_names, tmp_name)

    batch_df <- do.call(sdf_bind_rows, new_dfs[idx])
    spark_write_table(batch_df, tmp_name, mode = "overwrite")
    message(sprintf("  Materialized batch %d/%d (%d tables) -> %s",
                    b, length(batches), length(idx), tmp_name))
  }

  # Read back temp tables and union with cleaned existing data
  all_parts <- list(cleaned_df)
  for (tmp_name in temp_names) {
    all_parts <- c(all_parts, list(tbl(sc, tmp_name)))
  }

  final_df <- do.call(sdf_bind_rows, all_parts)
  spark_write_table(final_df, target_table, mode = "overwrite",
                    options = list("overwriteSchema" = "true"))

  # Clean up temp tables
  for (tmp_name in temp_names) {
    DBI::dbExecute(sc, sprintf("DROP TABLE IF EXISTS %s", tmp_name))
  }
  message(sprintf("  Cleaned up %d temp table(s)", length(temp_names)))
}
