# Databricks notebook source
library(purrr)
library(dplyr)
library(httr)
library(stringr)
library(tibble)

# COMMAND ----------

gh_list_dirs <- function(url) {
  resp <- GET(url)
  if (http_error(resp)) return(NULL)
  content(resp, as = "parsed")
}

build_valid_pairs_df <- function(gh_api_base, gh_path, gh_branch, gh_html_base) {
  country_url <- paste0(gh_api_base, "/", gh_path, "?ref=", gh_branch)

  items <- gh_list_dirs(country_url)
  if (is.null(items)) {
    return(tibble(country = character(), survey_clean = character(), gh_url = character()))
  }

  countries <- items %>%
    keep(~ .x$type == "dir") %>%
    map_chr("name") %>%
    keep(~ str_detect(.x, "^[A-Z]{3}$")) %>%
    sort()

  pairs <- map(countries, function(cntry) {
    url <- paste0(gh_api_base, "/", gh_path, "/", cntry, "?ref=", gh_branch)
    entries <- gh_list_dirs(url)
    if (is.null(entries)) return(NULL)

    dirs <- entries %>%
      keep(~ .x$type == "dir") %>%
      map_chr("name")

    if (length(dirs) == 0) return(NULL)

    tibble(country = cntry, survey_clean = dirs)
  })

  bind_rows(pairs) %>%
    mutate(
      gh_url = if_else(
        nzchar(survey_clean),
        paste0(gh_html_base, "/", country, "/", survey_clean),
        NA_character_
      )
    )
}
