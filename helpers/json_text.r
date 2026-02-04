# Databricks notebook source
# Organizations
GLD_TEAM_NAME  <- "Economic Policy - Growth and Jobs Unit"
WB_AFFIL <- "World Bank"
DECDG_NAME <- "Development Data Group"
DECDG_ABBR <- "DECDG"
GLD_MAIL <- "gld@worldbank.org"

# Roles
ROLE <- "Metadata and data deposit"
ROLE_STUDY_DOCS <- "Documentation of the study"
ROLE_PRODUCERS <- "Production of the harmonized file"
ROLE_SURVEY_PROD <- "Production of the survey data"
GLD_NAME_LONG <- "Global Labour Database Harmonized Dataset"

# Series
SERIES_NAME <- "Other Household Survey[hh / oth]"
SERIES_INFO <- paste(
  "The Global Labor Database (GLD) builds on the work done under the \"International Income Distribution Database\" (I2D2) project.",
  "Both are efforts to harmonize diverse household surveys to a common standard.",
  "I2D2 was discontinued in 2019 and followed by the Global Monitoring Database (GMD) and GLD.",
  "The former harmonized Income and Expenditure surveys that enable poverty estimates.",
  "GLD focuses mainly on household surveys not covered by GMD that still contain valuable labour market information (e.g., labour force surveys).",
  "Both GMD and GLD share a common data dictionary so that variables can be compared across databases.",
  "GLD has no established release calendar but aims to keep expanding continuously to provide users a global and up to date suite of microdata.",
  sep = " "
)

ABSTRACT_BASE <- paste(
  "Statistical agencies in nearly every nation conduct household surveys which provide individual-level information.",
  "However, the datasets are often difficult to access and not readily comparable across countries or across time.",
  "This greatly hampers their use for comparative studies of developing economies.",
  "The Global Labor Datase (GLD) is a worldwide collection of standardised microdata drawn from representative household surveys and consisting of a standardized set of geographic, sociodemographic, migration, education, and labour market variables.",
  "GLD draws on different types of surveys, usually conducted by national statistical agencies, like Labour Force Surveys, and multi-topic surveys (such as Living Standards Measurement Study Surveys).",
  "GLD aims to be a collaborative and reactive database that allows researchers to not just use the data but to share with them every step from raw survey to harmonized output.",
  "GLD allows cross-country comparisons and analysis at various disaggregation levels: gender, urban-rural, age cohorts, or employment status.",
  sep = " "
)

METHOD_NOTES <- paste(
  "The GLD data harmonization process generates variables from the raw survey data or, in some cases, from regional harmonized files if raw surveys cannot be obtained.",
  "Harmonizers review survey questionnaires and methodology documents and code following a standardised template.",
  "The output is evaluated both using detailed quality checks procedure and, whenever possible, reaching out to colleagues in the respective countries for validation.",
  sep = " "
)

CITATION_REQUIREMENTS <- function(survey_extended, year, filename) {
  paste0(
    "Use of the dataset must be acknowledged using a citation which would include:\n",
    "- the Identification of the Primary Investigator\n",
    "- the title of the survey (including country, acronym and year of implementation)\n",
    "- the survey reference number\n",
    "- the source and date of download\n\n",
    "Example:\n",
    "World Bank. ", survey_extended, " ", year,
    ", Global Labour Database Harmonized Dataset. Ref: ", filename,
    ". Dataset downloaded from [url] on [date]."
  )
}

BIB_CITATION <- function(survey_extended, year, filename) {
  paste0(
    "World Bank. ", survey_extended, " ", year,
    ", Global Labour Database Harmonized Dataset. Ref: ", filename,
    ". Dataset downloaded from [url] on [date]."
  )
}

DISCLAIMER <- paste(
  "The user of the data acknowledges that the original collector of the data, the authorized distributor of the data,",
  "and the relevant funding agency bear no responsibility for use of the data or for interpretations or inferences based upon such uses.",
  sep = " "
)

#Text for harmonized tables
GEOG_COVERAGE_HARMONIZED = "International"
PRODUCERS_NAME_HARMONIZED = "National Statistical Offices of the featured countries"
