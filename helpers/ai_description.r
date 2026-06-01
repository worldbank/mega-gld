# Databricks notebook source
library(httr)
library(jsonlite)
library(readxl)


# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

if (is_databricks()) {
  suppressMessages(suppressWarnings({
    install.packages(c("reticulate", "officer"))
    reticulate::py_install("pdfplumber", pip = TRUE)
  }))
}

# COMMAND ----------

if (!exists("is_databricks")) {
  source("helpers/config.r")
}

# COMMAND ----------


make_token_provider <- function() {
  token      <- NULL
  expires_at <- 0

  function() {
    now <- as.numeric(Sys.time())
    if (is.null(token) || now >= expires_at) {
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
      content    <- httr::content(resp, as = "parsed", encoding = "UTF-8")
      token      <<- content$access_token
      expires_at <<- now + as.numeric(content$expires_in) - 60
    }
    token
  }
}

get_token <- make_token_provider()


call_azure_openai <- function(system_msg, user_msg, token_provider, max_attempts = 5) {
  for (attempt in seq_len(max_attempts)) {
    resp <- httr::POST(
      paste0(
        "https://azapim.worldbank.org/conversationalai/v2/",
        "openai/deployments/gpt-5/chat/completions?api-version=2025-04-01-preview"
      ),
      httr::add_headers(
        Authorization  = paste("Bearer", token_provider()),
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

    status <- httr::status_code(resp)

    if (status == 429) {
      body       <- httr::content(resp, as = "parsed", encoding = "UTF-8")
      error_code <- body$error$code
      if (identical(error_code, "quota_exceeded")) {
        stop("Token quota exhausted, cannot retry: ", body$error$message)
      }
      retry_after <- as.numeric(httr::headers(resp)[["retry-after"]])
      wait        <- if (!is.na(retry_after) && retry_after > 0) retry_after else 60
      message(sprintf("Rate limited (attempt %d/%d), waiting %ds...", attempt, max_attempts, wait))
      Sys.sleep(wait)
      next
    }

    if (status >= 300) {
      stop("Azure OpenAI call failed: ", httr::content(resp, as = "text", encoding = "UTF-8"))
    }

    parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
    raw    <- parsed$choices[[1]]$message$content
    lines  <- strsplit(trimws(raw), "\n")[[1]]
    lines  <- lines[nzchar(trimws(lines))]

    title       <- if (length(lines) >= 1) trimws(lines[[1]]) else NA_character_
    description <- if (length(lines) >= 2) trimws(lines[[2]]) else NA_character_

    if (is.na(title) || is.na(description)) {
      message("Unexpected model response (raw): ", raw)
    }

    return(list(title = title, description = description))
  }

  stop(sprintf("Azure OpenAI call failed after %d attempts (rate limit not resolved)", max_attempts))
}



extract_file_text <- function(path, max_chars = 300) {
  ext <- tolower(tools::file_ext(path))
  text <- tryCatch({
    if (ext == "pdf") {
      pdfplumber <- reticulate::import("pdfplumber")
      pdf        <- pdfplumber$open(path)
      pages      <- sapply(pdf$pages[1:min(2, length(pdf$pages))], function(p) {
        t <- p$extract_text()
        if (is.null(t)) "" else t
      })
      pdf$close()
      paste(pages, collapse = "\n")
    } else if (ext %in% c("xlsx", "xls")) {
      sheet <- suppressMessages(readxl::read_excel(path, n_max = 10))
      paste(
        paste(names(sheet), collapse = " "),
        paste(apply(sheet, 1, paste, collapse = " "), collapse = " ")
      )
    } else if (ext == "docx") {
      doc      <- officer::read_docx(path)
      content  <- officer::docx_summary(doc)
      text_rows <- content[content$content_type == "paragraph", "text"]
      paste(text_rows, collapse = " ")
    } else {
      paste(readLines(path, n = 50, warn = FALSE), collapse = " ")
    }
  }, error = function(e) "")
  substr(trimws(text), 1, max_chars)
}



get_ai_description_tech <- function(file_path, token_provider = get_token) {
  if (grepl("readme", basename(file_path), ignore.case = TRUE)) {
    return(list(
      title       = "README for the Harmonized Dataset",
      description = "Overview of dataset origin and access scope"
    ))
  }

  text <- extract_file_text(file_path, max_chars = 800)

  if (nchar(trimws(text)) < 50) {
    message("Insufficient text to generate metadata for file: ", file_path)
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

  result <- call_azure_openai(
    system_msg     = AZURE_OPENAI_SYSTEM_PROMPT,
    user_msg       = prompt,
    token_provider = token_provider
  )

  if (is.na(result$title) || is.na(result$description)) {
    message("NA metadata for file: ", file_path)
  }

  result
}


get_ai_description_quest <- function(file_path, token_provider = get_token) {
  text <- extract_file_text(file_path, max_chars = 800)

  if (nchar(trimws(text)) < 50) {
    message("Insufficient text to generate metadata for file: ", file_path)
    return(list(title = NA_character_, description = NA_character_))
  }

  prompt <- paste0(
    "File: ", basename(file_path), "\n\n",
    "A survey questionnaire file contains the following extracted text:\n\n",
    "---\n", text, "\n---\n\n",
    "Return exactly two lines:\n",
    "Line 1: A title of at most 8 words in title case. ",
    "Do not include the country name or the year. ",
    "The title should be in English. ",
    "If the filename suggests a time period (e.g. quarter, semester, wave), include it. ",
    "If the questionnaire is not in English, append the language in brackets, e.g. 'Household Survey Module A [French]'.\n",
    "Line 2: A description of at most 20 words. ",
    "If the questionnaire is not in English, mention the language. ",
    "The description should be in English. ",
    "Focus on what the questionnaire covers, not on what is absent or incomplete.\n",
    "Return nothing else."
  )

  result <- call_azure_openai(
    system_msg     = AZURE_OPENAI_SYSTEM_PROMPT,
    user_msg       = prompt,
    token_provider = token_provider
  )

  if (is.na(result$title) || is.na(result$description)) {
    message("NA metadata for file: ", file_path)
  }

  result
}


get_ai_description_data <- function(file_path, token_provider = get_token) {
  text <- extract_file_text(file_path, max_chars = 800)

  if (nchar(trimws(text)) < 50) {
    message("Insufficient text to generate metadata for file: ", file_path)
    return(list(title = NA_character_, description = NA_character_))
  }

  prompt <- paste0(
    "File: ", basename(file_path), "\n\n",
    "An additional data file for a labor survey dataset contains the following extracted text:\n\n",
    "---\n", text, "\n---\n\n",
    "Return exactly two lines:\n",
    "Line 1: A title of at most 8 words in title case summarising what this file contains. ",
    "Do not include the country name or the year.\n",
    "The title should be in English. ",
    "Line 2: A description of at most 20 words expanding on the title. ",
    "Focus on what the file contains, not on what is absent or incomplete.\n",
    "The description should be in English. ",
    "Return nothing else."
  )

  result <- call_azure_openai(
    system_msg     = AZURE_OPENAI_SYSTEM_PROMPT,
    user_msg       = prompt,
    token_provider = token_provider
  )

  if (is.na(result$title) || is.na(result$description)) {
    message("NA metadata for file: ", file_path)
  }

  result
}

# COMMAND ----------


