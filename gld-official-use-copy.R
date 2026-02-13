library(sparklyr)
library(dplyr)

source("helpers/config.R")

sc <- spark_connect(method = "databricks")

metadata <- tbl(sc, METADATA_TABLE) %>% collect()

to_copy_sdf <- metadata %>%
  filter(classification == "Official Use") %>%
  distinct(table_name)

# --- check tables that already exist in OUO ---
ouo_existing_sdf <- DBI::dbGetQuery(sc,paste0("SHOW TABLES IN ", OUO_SCHEMA)) %>%
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
      "CREATE TABLE IF NOT EXISTS ", OUO_SCHEMA, ".", t,
      " AS SELECT * FROM ", TARGET_SCHEMA, ".", t
    )
  )
}