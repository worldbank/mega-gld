# Databricks notebook source


# COMMAND ----------

# Environment detection
is_databricks <- function() {
  nzchar(Sys.getenv("DATABRICKS_RUNTIME_VERSION")) ||
    nzchar(Sys.getenv("DB_HOME")) ||
    nzchar(Sys.getenv("DATABRICKS_CLUSTER_ID"))
}

IN_DATABRICKS <- is_databricks()

# Paths
ROOT_DIR <- "/Volumes/prd_csc_mega/sgld48/vgld48/Documents"
JSON_DIR <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/json_to_publish"

# Database
TARGET_SCHEMA  <- "prd_csc_mega.sgld48"
OUO_SCHEMA     <- "prd_mega.sgld48"
METADATA_TABLE <- paste0(TARGET_SCHEMA, "._ingestion_metadata")

# GitHub (for documentation links)
GH_OWNER  <- "worldbank"
GH_REPO   <- "gld"
GH_BRANCH <- "main"
GH_PATH   <- "Support/B%20-%20Country%20Survey%20Details"
GH_API_BASE <- paste0("https://api.github.com/repos/", GH_OWNER, "/", GH_REPO, "/contents")
GH_HTML_BASE <- paste0("https://github.com/", GH_OWNER, "/", GH_REPO, "/tree/", GH_BRANCH, "/", GH_PATH)

# MDL
METADATA_API_BASE <- "https://metadataeditor.worldbank.org/index.php/api/"
REPOSITORY_ID <- 824
CATALOG_CONN_ID <- 43

if (IN_DATABRICKS) {
  ME_API_KEY <- dbutils.secrets.get("GLDKEYVAULT","NADA_API_KEY")
} else {
  if (requireNamespace("dotenv", quietly = TRUE)) {
    dotenv::load_dot_env()
    }
  ME_API_KEY <- Sys.getenv("NADA_API_KEY")
}

#API Integration Tests Config
RUN_API_INTEGRATION = False
