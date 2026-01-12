library(sparklyr)
library(dplyr)

sc <- spark_connect(method = "databricks")

metadata_table <- "prd_csc_mega.sgld48._ingestion_metadata"
csc_schema <- "prd_csc_mega.sgld48"
ouo_schema <- "prd_mega.sgld48"

metadata <- tbl(sc, metadata_table) %>% collect()

to_copy_sdf <- metadata %>%
  filter(classification == "Official Use") %>%
  distinct(table_name)

# --- check tables that already exist in OUO ---
ouo_existing_sdf <- DBI::dbGetQuery(sc,paste0("SHOW TABLES IN ", ouo_schema)) %>%
  transmute(table_name = tableName) %>%
  distinct()

# --- identify missing tables ---
missing <- to_copy_sdf %>%
  anti_join(ouo_existing_sdf, by = "table_name") %>%
  collect() %>%
  pull(table_name)

# --- copy ---
for (t in missing) {
  DBI::dbExecute(
    sc,
    paste0(
      "CREATE TABLE IF NOT EXISTS ", ouo_schema, ".", t,
      " AS SELECT * FROM ", csc_schema, ".", t
    )
  )
}