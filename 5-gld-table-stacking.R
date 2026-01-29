library(sparklyr)
library(dplyr)
library(DBI)
library(rlang)

config <- spark_config()
config$spark.databricks.delta.schema.autoMerge.enabled <- "true"

sc <- spark_connect(conf=config, method = "databricks")

TABLE_QULIFIER <- "prd_csc_mega.sgld48."
metadata_table <- paste0(TABLE_QULIFIER, "_ingestion_metadata")
metadata <- tbl(sc, metadata_table) %>%
  collect()
OFFICIAL_CLASS <- 'Official Use'


# The comprehensive list of columns for the final table
schema_columns <- list(
  countrycode = "string",
  survname = "string",
  survey = "string",
  icls_v = "string",
  isced_version = "string",
  isco_version = "string",
  isic_version = "string",
  year = "integer",
  vermast = "string",
  veralt = "string",
  harmonization = "string",
  int_year = "integer",
  int_month = "integer",
  hhid = "string",
  pid = "string",
  weight = "double",
  weight_m = "double",
  weight_q = "double",
  psu = "string",
  ssu = "string",
  wave = "string",
  panel = "string",
  visit_no = "integer",
  urban = "integer",
  subnatidsurvey = "string",
  strata = "string",
  hsize = "double",
  age = "double",
  male = "integer",
  relationharm = "integer",
  relationcs = "string",
  marital = "integer",
  eye_dsablty = "integer",
  hear_dsablty = "integer",
  walk_dsablty = "integer",
  conc_dsord = "integer",
  slfcre_dsablty = "integer",
  comm_dsablty = "integer",
  migrated_mod_age = "integer",
  migrated_ref_time = "integer",
  migrated_binary = "integer",
  migrated_years = "integer",
  migrated_from = "integer",
  migrated_from_cat = "integer",
  migrated_from_code = "integer",
  migrated_from_country = "string",
  migrated_reason = "integer",
  ed_mod_age = "integer",
  school = "integer",
  literacy = "integer",
  educy = "integer",
  educat7 = "integer",
  educat5 = "integer",
  educat4 = "integer",
  educat_orig = "string",
  educat_isced = "double",
  vocational = "integer",
  vocational_type = "integer",
  vocational_length_l = "integer",
  vocational_length_u = "integer",
  vocational_financed = "integer",
  vocational_field = "integer",
  minlaborage = "double",
  lstatus = "integer",
  potential_lf = "integer",
  underemployment = "integer",
  nlfreason = "integer",
  unempldur_l = "double",
  unempldur_u = "double",
  empstat = "integer",
  ocusec = "integer",
  industry_orig = "string",
  industrycat_isic = "string",
  industrycat10 = "integer",
  industrycat4 = "integer",
  occup_orig = "string",
  occup_isco = "string",
  occup_skill = "integer",
  occup = "integer",
  wage_no_compen = "double",
  unitwage = "integer",
  whours = "double",
  wmonths = "double",
  wage_total = "double",
  contract = "integer",
  healthins = "integer",
  socialsec = "integer",
  union = "integer",
  firmsize_l = "double",
  firmsize_u = "double",
  empstat_2 = "integer",
  ocusec_2 = "integer",
  industry_orig_2 = "string",
  industrycat_isic_2 = "integer",
  industrycat10_2 = "integer",
  industrycat4_2 = "integer",
  occup_orig_2 = "string",
  occup_isco_2 = "string",
  occup_skill_2 = "integer",
  occup_2 = "integer",
  wage_no_compen_2 = "double",
  unitwage_2 = "integer",
  whours_2 = "double",
  wmonths_2 = "double",
  wage_total_2 = "double",
  firmsize_l_2 = "double",
  firmsize_u_2 = "double",
  t_hours_others = "double",
  t_wage_nocompen_others = "double",
  t_wage_others = "double",
  t_hours_total = "double",
  t_wage_nocompen_total = "double",
  t_wage_total = "double",
  lstatus_year = "integer",
  potential_lf_year = "integer",
  underemployment_year = "integer",
  nlfreason_year = "integer",
  unempldur_l_year = "double",
  unempldur_u_year = "double",
  empstat_year = "integer",
  ocusec_year = "integer",
  industry_orig_year = "string",
  industrycat_isic_year = "integer",
  industrycat10_year = "integer",
  industrycat4_year = "integer",
  occup_orig_year = "string",
  occup_isco_year = "string",
  occup_skill_year = "integer",
  occup_year = "integer",
  wage_no_compen_year = "double",
  unitwage_year = "integer",
  whours_year = "double",
  wmonths_year = "double",
  wage_total_year = "double",
  contract_year = "integer",
  healthins_year = "integer",
  socialsec_year = "integer",
  union_year = "integer",
  firmsize_l_year = "double",
  firmsize_u_year = "double",
  empstat_2_year = "integer",
  ocusec_2_year = "integer",
  industry_orig_2_year = "string",
  industrycat_isic_2_year = "integer",
  industrycat10_2_year = "integer",
  industrycat4_2_year = "integer",
  occup_orig_2_year = "string",
  occup_isco_2_year = "string",
  occup_skill_2_year = "integer",
  occup_2_year = "integer",
  wage_no_compen_2_year = "double",
  unitwage_2_year = "integer",
  whours_2_year = "double",
  wmonths_2_year = "double",
  wage_total_2_year = "double",
  firmsize_l_2_year = "double",
  firmsize_u_2_year = "double",
  t_hours_others_year = "double",
  t_wage_nocompen_others_year = "double",
  t_wage_others_year = "double",
  t_hours_total_year = "double",
  t_wage_nocompen_total_year = "double",
  t_wage_total_year = "double",
  njobs = "double",
  t_hours_annual = "double",
  linc_nc = "double",
  laborincome = "double"
)

expected_cols <- names(schema_columns)

# TODO add this to the metadata table as a flag
TO_REMOVE <- c('MEX_2023_ENOE_Panel_V01_M_V01_A_GLD_ALL', 'IND_2022_PLFS_Urban_Panel_V01_M_V01_A_GLD_ALL')
# Delete the harmonized Delta table if it already exists
HARMONIZED_CONFIDENTIAL <- paste0(TABLE_QULIFIER, "GLD_HARMONIZED_ALL")
HARMONIZED_OFFICIAL <- paste0(TABLE_QULIFIER, "GLD_HARMONIZED_OUO")

# Build the column definitions: "countrycode STRING, survname STRING..."
columns_sql <- paste(names(schema_columns), toupper(unlist(schema_columns)), collapse = ", ")

# The SQL command
create_query_confidential <- paste0("CREATE TABLE ", HARMONIZED_CONFIDENTIAL, " (", columns_sql, ") USING DELTA")
create_query_official <- paste0("CREATE TABLE ", HARMONIZED_OFFICIAL, " (", columns_sql, ") USING DELTA")

# Execute
DBI::dbExecute(sc, paste0("DROP TABLE IF EXISTS ", HARMONIZED_CONFIDENTIAL))
DBI::dbExecute(sc, create_query_confidential)

DBI::dbExecute(sc, paste0("DROP TABLE IF EXISTS ", HARMONIZED_OFFICIAL))
DBI::dbExecute(sc, create_query_official)

subnational_pattern <- "^subnatid\\d+(_prev)?$"
gaul_pattern <- "^gaul_adm\\d+_code$"

# Get distinct table names from the ingestion metadata where ingested is TRUE
table_metadata <- metadata %>%
  filter(ingested == TRUE) %>%
  select(table_name, classification, country, year, survey) %>%
  distinct() %>%
  collect()


DBI::dbExecute(sc, "SET spark.databricks.delta.schema.autoMerge.enabled = true")
for (i in 1:nrow(table_metadata)) {
  tbl <- table_metadata$table_name[i]
  classification <- table_metadata$classification[i]
  country_val <- table_metadata$country[i]
  survey_val <- table_metadata$survey[i]
  
  if (tbl %in% TO_REMOVE) next
  
  print(paste("Processing:", tbl))
  src_df <- tbl(sc, paste0(TABLE_QULIFIER, tbl))
  src_cols <- colnames(src_df)
  
  # 1. Build the list of expressions for the select statement
  mutate_exprs <- list()

  for (col_name in expected_cols) {
    target_type <- schema_columns[[col_name]]
    
    if (col_name == "countrycode") {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", country_val, "' AS ", target_type, ")"))
    } else if (col_name == "survname") {
      mutate_exprs[[col_name]] <- sql(paste0("CAST('", survey_val, "' AS ", target_type, ")"))
    } else if (col_name %in% src_cols) {
      mutate_exprs[[col_name]] <- sql(paste0("CAST(", col_name, " AS ", target_type, ")"))
    } else {
      mutate_exprs[[col_name]] <- sql(paste0("CAST(NULL AS ", target_type, ")"))
    }
  }
  
  # 2. Handle Dynamic Columns (Patterns)
  dynamic_cols <- src_cols[grepl(subnational_pattern, src_cols) | grepl(gaul_pattern, src_cols)]
  dynamic_cols <- setdiff(dynamic_cols, expected_cols)
  
  for (dc in dynamic_cols) {
      mutate_exprs[[dc]] <- sql(paste0("CAST(", dc, " AS string)"))
    }
  final_column_names <- c(expected_cols, dynamic_cols)

  extra_cols <- setdiff(src_cols, final_column_names)
  print(paste(extra_cols, collapse=", "))
  if(length(extra_cols) > 0) print(paste("Extra columns found in", tbl, ":", paste(extra_cols, collapse=", ")))

  # 3. Apply the transformation in ONE single step
  # This keeps the Spark Execution Plan small and fast
  aligned_df <- src_df %>%
    mutate(!!!mutate_exprs) %>%
    select(all_of(final_column_names))  
  # 1. Register the dataframe as a temporary view so the SQL engine can see it
  sdf_register(aligned_df, "v_to_append")

 # 3. Handle the Confidential Table
  # Use MERGE with 1=0 (The "Trick" to force an append)
  # This will now work with new columns thanks to autoMerge.enabled
  DBI::dbExecute(sc, paste0("
    MERGE INTO ", HARMONIZED_CONFIDENTIAL, " AS target
    USING v_to_append AS source
    ON 1 = 0
    WHEN NOT MATCHED THEN INSERT *
  "))

  # 4. Repeat for Official Table
  if (!is.na(classification) && classification == OFFICIAL_CLASS) {
    DBI::dbExecute(sc, paste0("
      MERGE INTO ", HARMONIZED_OFFICIAL, " USING v_to_append ON 1=0
      WHEN NOT MATCHED THEN INSERT *
    "))
  }
}