library(dplyr)
library(jsonlite)
library(stringr)
library(purrr)
library(sparklyr)
library(httr)
library(readxl)

# --- set variables --- 

OWNER  <- "worldbank"
REPO   <- "gld"
PATH   <- "Support/B%20-%20Country%20Survey%20Details"
BASE   <- paste0("https://api.github.com/repos/", OWNER, "/", REPO, "/contents")
BRANCH <- "main"

root_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Documents"

sc <- spark_connect(method = "databricks")

target_schema  <- "prd_csc_mega.sgld48"
metadata_table <- paste0(target_schema, "._ingestion_metadata")


# --- import metadata --- 

metadata <- tbl(sc, metadata_table) %>% collect()

unpublished <- metadata %>% 
  filter(published == FALSE)

unpublished

# --- import supporting files ---
countries_names <- tbl(sc, "prd_mega.indicator.country") %>%
  select(code = country_code,name = country_name) %>%
  collect()

path_survey <- file.path(root_dir, "survey-metadata.xlsx")
survey <- read_excel(path_survey)

merged_df <- left_join(
  unpublished,
  survey,
  by = c("survey", "country")
)


merged_df <- merged_df %>%
  mutate(survey_clean = str_extract(survey, "^[^-]+"))



# --- retrieve valid gh links ---

country_url <- paste0(BASE, "/", PATH, "?ref=", BRANCH)

resp <- GET(country_url)
stop_for_status(resp)
items <- content(resp, as = "parsed")

countries <- items %>%
  keep(~ .x$type == "dir") %>%
  map_chr("name") %>%
  keep(~ str_detect(.x, "^[A-Z]{3}$")) %>%
  sort()

valid_pairs <- list()

for (c in countries) {
  url <- paste0(BASE, "/", PATH, "/", c, "?ref=", BRANCH)
  r <- GET(url)
  if (http_error(r)) next
  entries <- content(r, as = "parsed")
  dirs <- entries %>%
    keep(~ .x$type == "dir") %>%
    map_chr("name")
  if (length(dirs) > 0) {
    valid_pairs <- append(valid_pairs, lapply(dirs, function(x) c(country = c, survey_clean = x)))
  }
}

valid_pairs_df <- bind_rows(lapply(valid_pairs, function(x) {
  tibble(country = x[1], survey_clean = x[2])
}))


base_url <- paste0("https://github.com/", OWNER, "/", REPO,"/tree/main/", PATH)

valid_pairs_df <- valid_pairs_df %>% 
  mutate(
    gh_url = if_else(
      !is.na(survey_clean),
      paste0(base_url, "/", country, "/", survey_clean),
      NA_character_
    )
  )


merged_df <- merged_df %>%
  left_join(valid_pairs_df,
            by = c("country", "survey_clean")) 


# --- helper functions ---
safe <- function(x) ifelse(is.na(x) | x == "", "", x)
date_prod <- function(x) format(as.Date(x), "%m/%d/%Y")
date_prod_year <- function(x) format(as.Date(x), "%Y")
date_prod_year_month <- function(x) format(as.Date(x), "%Y-%m")
trim <- function(x) str_trim(as.character(x))


make_mdl_json <- function(row) {

  # ---- dates ----
  prod_date <- Sys.Date()
  prod_mmddyyyy <- date_prod(prod_date)
  prod_y  <- date_prod_year(prod_date)
  prod_ym <- date_prod_year_month(prod_date)

  # ---- quarter logic ----
  q <- safe(row$quarter)
  q_piece <- ifelse(q == "", "", paste0(" ", q))

  # --- other repeatables ---
  A_version_padded <- sprintf("%02d", as.integer(row$A_version))
  gld_team_name <- "Economic Policy - Growth and Jobs Unit"
  idno_val <- paste0("DDI_", row$filename, "_WB")
  long_title <- paste0(row$survey_extended, q_piece, " ", row$year, ", Global Labour Database Harmonized Dataset")

  # ---- GH link ----
  gh_link <- safe(row$gh_url)
  abstract_tail <- ifelse(
    gh_link == "",
    "",
    paste0("\n\nFor more details see --> ", gh_link)
  )


  # ---- country lookup ----
  nation_name <- countries_names$name[match(row$country, countries_names$code)]

  # --- populate the json ---
  json <- list(
    idno = idno_val,
    collection_ids = list(824),
    template_uid = "microdata-system-en",
    overwrite = "no",

    doc_desc = list(
      title = row$filename,
      idno  = idno_val,
      producers = list(
        list(
          name = gld_team_name,
          abbreviation = "",
          affiliation = "World Bank",
          role = "Metadata and data deposit"
        ),
        list(
          name = "Development Data Group",
          abbreviation = "DECDG",
          affiliation = "World Bank",
          role = "Documentation of the study"
        )
      ),
      prod_date = prod_mmddyyyy,
      version_statement = list(
        version = paste0("A", A_version_padded),
        version_date = prod_mmddyyyy,
        version_resp = gld_team_name,
        version_notes = safe(row$version_label)
      )
    ),

    study_desc = list(
      title_statement = list(
        idno = idno_val,
        title = long_title,
        alternate_title = paste0(row$survey_clean, " GLD ", row$year)
      ),
      authoring_entity = list(
        list(
          name = gld_team_name,
          affiliation = "World Bank"
        )
      ),
      production_statement = list(
        producers = list(
          list(
            name = gld_team_name,
            affiliation = "The World Bank",
            role = "Production of the harmonized file"
          ),
          list(
            name = if (!is.null(row$producers_name) && !is.na(row$producers_name)&& nzchar(trimws(row$producers_name))) {row$producers_name } else { paste("National Statistical Offices of", nation_name)},
            affiliation = "",
            role = "Production of the survey data"
          )
        ),
        funding_agencies = list(
          list(
            name = "World Bank",
            abbreviation = "",
            role = ""
          )
        )
      ),

      distribution_statement = list(
        contact = list(
          list(
            name = gld_team_name,
            affiliation = "World Bank",
            email = "gld@worldbank.org",
            uri = ""
          )
        ),
        depositor = list(
          list(
            name = gld_team_name,
            abbreviation = "",
            affiliation = ""
          )
        )
      ),

      series_statement = list(
        series_name = "Other Household Survey[hh / oth]",
        series_info =
          "The Global Labor Database (GLD) builds on the work done under the \"International Income Distribution Database\" (I2D2) project. Both are efforts to harmonize diverse household surveys to a common standard. I2D2 was discontinued in 2019 and followed by the Global Monitoring Database (GMD) and GLD. The former harmonized Income and Expenditure surveys that enable poverty estimates. GLD focuses mainly on household surveys not covered by GMD that still contain valuable labour market information (e.g., labour force surveys). Both GMD and GLD share a common data dictionary so that variables can be compared across databases. GLD has no established release calendar but aims to keep expanding continuously to provide users a global and up to date suite of microdata."
      ),

      version_statement = list(
        version = paste0("Version ", row$A_version,
                         ": Harmonized, anonymized dataset for, ", row$classification, "distribution."),
        version_date = prod_ym,
        version_notes = safe(row$version_label)
      ),

      bib_citation = paste0(
        "World Bank. ", row$survey_extended," ", row$year,", Global Labour Database Harmonized Dataset. Ref: ", row$filename,". Dataset downloaded from [url] on [date]."
      ),
      bib_citation_format = "text",

      study_info = list(
        abstract = paste0(
          "Statistical agencies in nearly every nation conduct household surveys which provide individual-level information. However, the datasets are often difficult to access and not readily comparable across countries or across time. This greatly hampers their use for comparative studies of developing economies. The Global Labor Datase (GLD) is a worldwide collection of standardised microdata drawn from representative household surveys and consisting of a standardized set of geographic, sociodemographic, migration, education, and labour market variables.
          GLD draws on different types of surveys, usually conducted by national statistical agencies, like Labour Force Surveys, and multi-topic surveys (such as Living Standards Measurement Study Surveys). GLD aims to be a collaborative and reactive database that allows researchers to not just use the data but to share with them every step from raw survey to harmonized output. GLD allows cross-country comparisons and analysis at various disaggregation levels: gender, urban-rural, age cohorts, or employment status.",
          abstract_tail
        ),

        coll_dates = list(
          list(
            start = prod_y,
            end   = prod_y,
            cycle = ""
          )
        ),

        nation = list(
          list(
            name = safe(nation_name),
            abbreviation = row$country
          )
        ),

        geog_coverage = if (!is.null(row$geog_coverage) && !is.na(row$geog_coverage) && nzchar(trimws(row$geog_coverage))) {row$geog_coverage} else {"National"},
        analysis_unit = if (isTRUE(row$household_level)) {
          "Individual and Household"
        } else {
          "Individual"
        }
      ),

      method = list(
        data_collection = list(
          method_notes =
            "The GLD data harmonization process generates variables from the raw survey data or, in some cases, from regional harmonized files if raw surveys cannot be obtained. Harmonizers review survey questionnaires and methodology documents and code following a standardised template. The output is evaluated both using detailed quality checks procedure and, whenever possible, reaching out to colleagues in the respective countries for validation."
        )
      ),

      data_access = list(
        dataset_use = list(
          conf_dec = list(
            list(txt="", required="", form_no="", uri="")
          ),
          contact = list(
            list(
              name = gld_team_name,
              affiliation = "World Bank",
              email = "",
              uri = ""
            )
          ),
          cit_req = paste0(
            "Use of the dataset must be acknowledged using a citation which would include:
            - the Identification of the Primary Investigator
            - the title of the survey (including country, acronym and year of implementation)
            - the survey reference number
            - the source and date of download

            Example:
            World Bank. ", row$survey_extended, row$year,
            ", Global Labour Database Harmonized Dataset. Ref: ", row$filename,
            ". Dataset downloaded from [url] on [date]."
          ),
          conditions = safe(row$data_access_note),
          disclaimer =
            "The user of the data acknowledges that the original collector of the data, the authorized distributor of the data, and the relevant funding agency bear no responsibility for use of the data or for interpretations or inferences based upon such uses."
        )
      )
    ),

    # data_files = list(
    #   list(
    #     file_id = "F1",
    #     file_name = row$filename
    #   )
    # ),

    #variables = "",

    # variable_groups = variable_groups,

    datacite = list(
      creators = list(
        list(name = gld_team_name)
      ),
      titles = list(
        list(title = long_title)
      ),
      types = list(
        resourceType = "Dataset"
      )
    ),

    tags = list(
      list(tag = "Labor"),
      list(tag = "")
    )
  )
  return(json)
}


# --- generate the json files --- []

json_dir <- "/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/json_to_publish/"

for (i in 1:nrow(merged_df)) {
  tryCatch({
    row <- merged_df[i, ]
    json_obj <- make_mdl_json(row)
    out_path <- paste0(json_dir, row$filename, ".json")
    write_json(json_obj, out_path, pretty = TRUE, auto_unbox = TRUE)
    message("JSON created for ", row$filename)
  }, error = function(e) {
      warning("FAILED to create JSON for ", row$filename ,conditionMessage(e))
  })
}



