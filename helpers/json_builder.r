# Databricks notebook source
library(stringr)


# COMMAND ----------

# MAGIC %run "./json_text"

# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}
if (!exists("GLD_TEAM_NAME")) {
  source("helpers/json_text.r")
}

# COMMAND ----------

make_mdl_json <- function(row, countries_names) {
  # ---- formatting helpers ----
  safe <- function(x) {
    x <- as.character(x)
    ifelse(is.na(x) | x == "", "", x)
  }

  date_mmddyyyy <- function(x) format(as.Date(x), "%m/%d/%Y")
  date_year     <- function(x) format(as.Date(x), "%Y")
  date_ym       <- function(x) format(as.Date(x), "%Y-%m")


  # ---- dates ----
  prod_date     <- Sys.Date()
  prod_mmddyyyy <- date_mmddyyyy(prod_date)
  prod_y        <- date_year(prod_date)
  prod_ym       <- date_ym(prod_date)

  # ---- quarter logic ----
  q <- safe(row$quarter)
  q_piece <- ifelse(q == "", "", paste0(" ", q))

  # --- other repeatables ---
  A_version_padded <- sprintf("%02d", as.integer(row$A_version))
  idno_val <- paste0("DDI_", row$filename, "_WB")
  long_title <- paste0(row$survey_extended, q_piece, " ", row$year, ", ", GLD_NAME_LONG)

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
    collection_ids = list(REPOSITORY_ID),
    template_uid = "microdata-system-en",
    type = "microdata",
    overwrite = "no",

    doc_desc = list(
      title = row$filename,
      idno  = idno_val,
      producers = list(
        list(
          name = GLD_TEAM_NAME,
          abbreviation = "",
          affiliation = WB_AFFIL,
          role = ROLE
        ),
        list(
          name = DECDG_NAME,
          abbreviation = DECDG_ABBR,
          affiliation = WB_AFFIL,
          role = ROLE_STUDY_DOCS
        )
      ),
      prod_date = prod_mmddyyyy,
      version_statement = list(
        version = paste0("A", A_version_padded),
        version_date = prod_mmddyyyy,
        version_resp = GLD_TEAM_NAME,
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
          name = GLD_TEAM_NAME,
          affiliation = WB_AFFIL
        )
      ),
      production_statement = list(
        producers = list(
          list(
            name = GLD_TEAM_NAME,
            affiliation = WB_AFFIL,
            role = ROLE_PRODUCERS
          ),
          list(
            name = if (!is.null(row$producers_name) && !is.na(row$producers_name)&& nzchar(trimws(row$producers_name))) {row$producers_name } else { paste("National Statistical Offices of", nation_name)},
            affiliation = "",
            role = ROLE_SURVEY_PROD
          )
        ),
        funding_agencies = list(
          list(
            name = WB_AFFIL,
            abbreviation = "",
            role = ""
          )
        )
      ),

      distribution_statement = list(
        contact = list(
          list(
            name = GLD_TEAM_NAME,
            affiliation = WB_AFFIL,
            email = GLD_MAIL,
            uri = ""
          )
        ),
        depositor = list(
          list(
            name = GLD_TEAM_NAME,
            abbreviation = "",
            affiliation = ""
          )
        )
      ),

      series_statement = list(
        series_name = SERIES_NAME,
        series_info = SERIES_INFO
      ),

      version_statement = list(
        version = paste0("Version ", row$A_version,
                         ": Harmonized, anonymized dataset for, ", row$classification, " distribution."),
        version_date = prod_ym,
        version_notes = safe(row$version_label)
      ),

      bib_citation = BIB_CITATION(
        survey_extended = row$survey_extended,
        year            = row$year,
        filename        = row$filename
      ),
      bib_citation_format = "text",

      study_info = list(
        abstract = paste0(ABSTRACT_BASE,
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
          method_notes = METHOD_NOTES
        )
      ),

      data_access = list(
        dataset_use = list(
          conf_dec = list(
            list(txt="", required="", form_no="", uri="")
          ),
          contact = list(
            list(
              name = GLD_TEAM_NAME,
              affiliation = WB_AFFIL,
              email = "",
              uri = ""
            )
          ),
          cit_req = CITATION_REQUIREMENTS(
            survey_extended = row$survey_extended,
            year            = row$year,
            filename        = row$filename
          ),
          conditions = safe(row$data_access_note),
          disclaimer = DISCLAIMER
        )
      )
    ),

    datacite = list(
      creators = list(
        list(name = GLD_TEAM_NAME)
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

