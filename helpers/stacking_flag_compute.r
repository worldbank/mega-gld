# Databricks notebook source
library(dplyr)
library(stringr)

compute_stacking <- function(df) {

  base <- df %>%
    mutate(
      stacking = 0L,
      is_panel = str_detect(str_to_lower(as.character(table_name)), "panel"),
      harm_prio = case_when(
        harmonization == "GLD"       ~ 2L,
        harmonization == "GLD-Light" ~ 1L,
        TRUE                         ~ 0L
      ),
      has_classification =
        !is.na(classification) & str_trim(as.character(classification)) != ""
    )

  annual_winners <- base %>%
    filter(
      !is_panel,
      is.na(quarter) | str_trim(as.character(quarter)) == "",
      has_classification
    ) %>%
    group_by(country, year) %>%
    arrange(desc(harm_prio), desc(M_version), desc(A_version), .by_group = TRUE) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    transmute(filename, stacking = 1L)

  quarterly_winners <- base %>%
    filter(
      !is_panel,
      !is.na(quarter),
      str_trim(as.character(quarter)) != "",
      has_classification
    ) %>%
    group_by(country, year, quarter) %>%
    arrange(desc(harm_prio), desc(M_version), desc(A_version), .by_group = TRUE) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    transmute(filename, stacking = 1L)

  base %>%
    left_join(bind_rows(annual_winners, quarterly_winners), by = "filename") %>%
    mutate(stacking = coalesce(stacking.y, stacking.x)) %>%
    select(-stacking.x, -stacking.y, -is_panel, -harm_prio, -has_classification)
}
