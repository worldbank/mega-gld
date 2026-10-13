# Databricks notebook source
# Environment detection
is_databricks <- function() {
  nzchar(Sys.getenv("DATABRICKS_RUNTIME_VERSION")) ||
    nzchar(Sys.getenv("DB_HOME")) ||
    nzchar(Sys.getenv("DATABRICKS_CLUSTER_ID"))
}

IN_DATABRICKS <- is_databricks()

# Parameters
# Every setting read through gld_param() can be overridden, in order of precedence, by:
#   1. a job parameter (or notebook widget) with the same name, e.g. target_schema.
#      databricks.yml sets these per target, so the staging job runs against the staging
#      schema and the sample folder without any code change (see README, Staging environment);
#   2. an environment variable named GLD_<NAME>, e.g. GLD_TARGET_SCHEMA, for interactive
#      runs and local tests;
#   3. the production default given here.
gld_param <- function(name, default) {
  if (IN_DATABRICKS && exists("dbutils.widgets.get")) {
    value <- tryCatch(dbutils.widgets.get(name), error = function(e) "")
    if (nzchar(value)) return(value)
  }
  value <- Sys.getenv(paste0("GLD_", toupper(name)))
  if (nzchar(value)) return(value)
  default
}

# Paths
# Folder crawled for <country>/<dataset>/<version>/Data/Harmonized/*.dta files;
# survey-metadata.xlsx must sit at its root
ROOT_DIR <- gld_param("root_dir", "/Volumes/prd_csc_mega/sgld48/vgld48/Documents")
# Folder where json files await publication
JSON_DIR <- gld_param("json_dir", "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/json_to_publish")
CSV_HARMONIZED <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/harmonized_csv"


# Performance
BATCH_SIZE <- 15

# Database
# Schema holding _ingestion_metadata, the survey tables and the gld_harmonized_* tables
TARGET_SCHEMA  <- gld_param("target_schema", "prd_csc_mega.sgld48")
# Schema holding gld_availability
# TODO: Replace prd_mega.gld with prd_mega.sgld48 once the service principal provision is complete
OUO_SCHEMA     <- gld_param("ouo_schema", "prd_mega.sgld48")
# Prefix of every table the pipeline writes (empty in production). The staging job sets it to
# keep its own copies of the tables next to the production ones, e.g. staging__ingestion_metadata
TABLE_PREFIX   <- gld_param("table_prefix", "")

# Full name of a table written by the pipeline, e.g. gld_table("tha_2021_lfs_q2")
gld_table <- function(name, schema = TARGET_SCHEMA) {
  paste0(schema, ".", TABLE_PREFIX, name)
}

METADATA_TABLE      <- gld_table("_ingestion_metadata")
HARMONIZED_ALL      <- gld_table("gld_harmonized_all")
HARMONIZED_OFFICIAL <- gld_table("gld_harmonized_ouo")

if (IN_DATABRICKS) {
  message(sprintf(
    "GLD config: target_schema=%s, ouo_schema=%s, table_prefix='%s', root_dir=%s, json_dir=%s",
    TARGET_SCHEMA, OUO_SCHEMA, TABLE_PREFIX, ROOT_DIR, JSON_DIR
  ))
}

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


