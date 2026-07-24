# Databricks notebook source
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
CSV_HARMONIZED <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/harmonized_csv"


# Performance
BATCH_SIZE <- 15

# Database
TARGET_SCHEMA  <- "prd_csc_mega.sgld48"
OUO_SCHEMA     <- "prd_mega.sgld48"
METADATA_TABLE <- paste0(TARGET_SCHEMA, "._ingestion_metadata")
HARMONIZED_ALL <- paste0(TARGET_SCHEMA, ".gld_harmonized_all")
HARMONIZED_OFFICIAL <- paste0(TARGET_SCHEMA, ".gld_harmonized_ouo")
TRACKER_TABLE <- paste0(TARGET_SCHEMA, "._harmonized_version_publication_tracker")

# GitHub (for documentation links)
GH_OWNER  <- "worldbank"
GH_REPO   <- "gld"
GH_BRANCH <- "main"
GH_PATH   <- "Support/B%20-%20Country%20Survey%20Details"
GH_API_BASE <- paste0("https://api.github.com/repos/", GH_OWNER, "/", GH_REPO, "/contents")
GH_HTML_BASE <- paste0("https://github.com/", GH_OWNER, "/", GH_REPO, "/tree/", GH_BRANCH, "/", GH_PATH)

# MDL Config
METADATA_API_BASE <- "https://metadataeditor.worldbank.org/index.php/api/"
MICRODATA_API_BASE <- "https://microdatalibqa.worldbank.org/index.php/api/"
REPOSITORY_ID <- 824
CATALOG_CONN_ID <- 57

if (IN_DATABRICKS) {
  ME_API_KEY <- dbutils.secrets.get("GLDKEYVAULT","ME_API_KEY")
  NADA_API_KEY <- dbutils.secrets.get("GLDKEYVAULT","NADA_API_KEY")
} else {
  if (requireNamespace("dotenv", quietly = TRUE)) {
    dotenv::load_dot_env()
    }
  ME_API_KEY <- Sys.getenv("ME_API_KEY")
  NADA_API_KEY <- Sys.getenv("NADA_API_KEY")
}


# AI Config
if (IN_DATABRICKS) {
  GPT_TENANT_ID       <- dbutils.secrets.get("DAPGPTKEYVAULT", "GPT-APIM-Tenant-ID")
  GPT_CLIENT_ID       <- dbutils.secrets.get("DAPGPTKEYVAULT", "GPT-APIM-Client-ID")
  GPT_CLIENT_SECRET   <- dbutils.secrets.get("DAPGPTKEYVAULT", "GPT-APIM-Client-Secret")
  GPT_TOKEN_SCOPE     <- dbutils.secrets.get("DAPGPTKEYVAULT", "GPT-APIM-Token-Cred")
} else {
  if (requireNamespace("dotenv", quietly = TRUE)) {
    dotenv::load_dot_env()
  }
  GPT_TENANT_ID       <- Sys.getenv("GPT_TENANT_ID")
  GPT_CLIENT_ID       <- Sys.getenv("GPT_CLIENT_ID")
  GPT_CLIENT_SECRET   <- Sys.getenv("GPT_CLIENT_SECRET")
  GPT_TOKEN_SCOPE     <- Sys.getenv("GPT_TOKEN_SCOPE")
}

#API Integration Tests Config
RUN_API_INTEGRATION <- FALSE


