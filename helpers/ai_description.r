# Databricks notebook source
library(httr)
library(jsonlite)

# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

# COMMAND ----------

get_azure_openai_token <- function() {
  resp <- httr::POST(
    paste0("https://login.microsoftonline.com/", GPT_TENANT_ID, "/oauth2/v2.0/token"),
    httr::timeout(60),
    body = list(
      grant_type    = "client_credentials",
      client_id     = GPT_CLIENT_ID,
      client_secret = GPT_CLIENT_SECRET,
      scope         = GPT_TOKEN_SCOPE
    ),
    encode = "form"
  )

  if (httr::status_code(resp) >= 300) {
    stop("Failed to obtain Azure OpenAI token: ", httr::content(resp, as = "text", encoding = "UTF-8"))
  }

  httr::content(resp, as = "parsed", encoding = "UTF-8")$access_token
}


extract_file_text <- function(path, max_chars = 300) {
  ext <- tolower(tools::file_ext(path))
  text <- tryCatch({
    if (ext == "pdf") {
      pdftools::pdf_text(path)[1]  # first page only
    } else {
      paste(readLines(path, n = 50, warn = FALSE), collapse = " ")
    }
  }, error = function(e) "")
  text <- iconv(text, to = "UTF-8", sub = "")
  substr(trimws(text), 1, max_chars)
}


get_ai_description <- function(file_path, token) {
  text <- extract_file_text(file_path)

  prompt <- paste0(
    "Below is an excerpt from a technical documentation file for a labor survey dataset.\n\n",
    "File: ", basename(file_path), "\n", text, "\n\n",
    "Write a description of at most 10 words summarising what this document covers. If the name of the file is Where is this data from - ReadMe, the description can just be README for the Harmonized Dataset."
  )

  resp <- httr::POST(
    paste0(
      "https://azapim.worldbank.org/conversationalai/v2/",
      "openai/deployments/gpt-5/chat/completions?api-version=2025-04-01-preview"
    ),
    httr::add_headers(
      Authorization  = paste("Bearer", token),
      `Content-Type` = "application/json"
    ),
    httr::timeout(60),
    body = jsonlite::toJSON(
      list(
        messages = list(
          list(role = "system", content = "You are an AI assistant that writes concise metadata descriptions for documents."),
          list(role = "user",   content = prompt)
        )
      ),
      auto_unbox = TRUE
    )
  )

  if (httr::status_code(resp) >= 300) {
    warning("AI description failed for ", basename(file_path), ": ", httr::content(resp, as = "text", encoding = "UTF-8"))
    return(NA_character_)
  }

  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  parsed$choices[[1]]$message$content
}


get_ai_descriptions <- function(file_paths, token) {
  file_paths <- as.character(file_paths)
  message("Generating descriptions for ", length(file_paths), " files...")
  descriptions <- lapply(file_paths, function(path) {
    message("  Processing: ", basename(path))
    get_ai_description(path, token)
  })
  setNames(descriptions, basename(file_paths))
}
