# Databricks notebook source
install.packages("pdftools")

# COMMAND ----------

library(httr)
library(jsonlite)
library(readxl)


# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

if (is_databricks()) {
  system("sudo apt-get install -y libpoppler-cpp-dev", intern = TRUE)
  library(pdftools)
}

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


call_azure_openai <- function(system_msg, user_msg, token) {
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
          list(role = "system", content = system_msg),
          list(role = "user",   content = user_msg)
        )
      ),
      auto_unbox = TRUE
    )
  )

  if (httr::status_code(resp) >= 300) {
    stop("Azure OpenAI call failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
  }

  parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
  raw    <- parsed$choices[[1]]$message$content
  lines  <- strsplit(trimws(raw), "\n")[[1]]
  lines  <- lines[nzchar(trimws(lines))]

  list(
    title       = if (length(lines) >= 1) trimws(lines[[1]]) else NA_character_,
    description = if (length(lines) >= 2) trimws(lines[[2]]) else NA_character_
  )
}



extract_file_text <- function(path, max_chars = 300) {
  ext <- tolower(tools::file_ext(path))
  text <- tryCatch({
    if (ext == "pdf") {
      pages <- pdftools::pdf_text(path)
      paste(pages[1:min(2, length(pages))], collapse = "\n")
    } else if (ext %in% c("xlsx", "xls")) {
      sheet <- suppressMessages(readxl::read_excel(path, n_max = 10))
      paste(
        paste(names(sheet), collapse = " "),
        paste(apply(sheet, 1, paste, collapse = " "), collapse = " ")
      )
    } else {
      paste(readLines(path, n = 50, warn = FALSE), collapse = " ")
    }
  }, error = function(e) "")
  substr(trimws(text), 1, max_chars)
}


get_ai_description_tech <- function(file_path, token) {
  if (grepl("readme", basename(file_path), ignore.case = TRUE)) {
    return(list(
      title       = "README for the Harmonized Dataset",
      description = "Overview of dataset origin and access scope"
    ))
  }

  text <- extract_file_text(file_path, max_chars = 800)

  if (nchar(trimws(text)) < 50) {
    return(list(title = NA_character_, description = NA_character_))
  }

  prompt <- paste0(
    "File: ", basename(file_path), "\n\n",
    "A technical documentation file for a labor survey dataset contains the following text:\n\n",
    "---\n", text, "\n---\n\n",
    "Return exactly two lines:\n",
    "Line 1: A title of at most 8 words in title case summarising what this document covers. ",
    "Use both the filename and the text content as context. ",
    "Do not include the country name or the year.\n",
    "The title should be in English.\n",
    "If the file is not in English, append the language in brackets, e.g. 'Household Survey Module A [French]'.\n",
    "Line 2: A description of at most 20 words expanding on the title. ",
    "Focus on what the document contains, not on what is absent or incomplete.\n",
    "The description should be in English.\n",
    "Return nothing else."
  )

  call_azure_openai(
    system_msg = "You are an AI assistant that writes concise metadata titles and descriptions for documents. Always return exactly two lines: a title, then a description. Describe what is present, never what is missing.",
    user_msg   = prompt,
    token      = token
  )
}


get_ai_description_quest <- function(file_path, token) {
  text <- extract_file_text(file_path, max_chars = 800)

  if (nchar(trimws(text)) < 50) {
    return(list(title = NA_character_, description = NA_character_))
  }

  prompt <- paste0(
    "File: ", basename(file_path), "\n\n",
    "A survey questionnaire file contains the following extracted text:\n\n",
    "---\n", text, "\n---\n\n",
    "Return exactly two lines:\n",
    "Line 1: A title of at most 8 words in title case. ",
    "Do not include the country name or the year. ",
    "The title should be in English",
    "If the filename suggests a time period (e.g. quarter, semester, wave), include it. ",
    "If the questionnaire is not in English, append the language in brackets, e.g. 'Household Survey Module A [French]'.\n",
    "Line 2: A description of at most 20 words. ",
    "If the questionnaire is not in English, mention the language. ",
    "The description should be in English",
    "Focus on what the questionnaire covers, not on what is absent or incomplete.\n",
    "Return nothing else."
  )

  call_azure_openai(
    system_msg = "You are an AI assistant that writes concise metadata titles and descriptions for survey questionnaires. Always return exactly two lines: a title, then a description. Describe what is present, never what is missing.",
    user_msg   = prompt,
    token      = token
  )
}


get_ai_description_data <- function(file_path, token) {
  text <- extract_file_text(file_path, max_chars = 800)

  if (nchar(trimws(text)) < 50) {
    return(list(title = NA_character_, description = NA_character_))
  }

  prompt <- paste0(
    "File: ", basename(file_path), "\n\n",
    "An additional data file for a labor survey dataset contains the following extracted text:\n\n",
    "---\n", text, "\n---\n\n",
    "Return exactly two lines:\n",
    "Line 1: A title of at most 8 words in title case summarising what this file contains. ",
    "Do not include the country name or the year.\n",
    "The title should be in English",
    "Line 2: A description of at most 20 words expanding on the title. ",
    "Focus on what the file contains, not on what is absent or incomplete.\n",
    "The description should be in English",
    "Return nothing else."
  )

  call_azure_openai(
    system_msg = "You are an AI assistant that writes concise metadata titles and descriptions for data files. Always return exactly two lines: a title, then a description. Describe what is present, never what is missing.",
    user_msg   = prompt,
    token      = token
  )
}
