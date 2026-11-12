# Databricks notebook source
packages <- c("reticulate", "R.utils", "readxl", "officer", "readr")
install.packages(packages[!packages %in% installed.packages()[, "Package"]])

# COMMAND ----------

if (!reticulate::py_module_available("pdfplumber")) {
  reticulate::py_install("pdfplumber", pip = TRUE)
}


# COMMAND ----------

library(reticulate)
library(R.utils)
library(readxl)
library(officer)
library(readr)


# COMMAND ----------

read_pdf_text <- function(path, timeout = NULL) {
  run <- function() {
    pdfplumber <- reticulate::import("pdfplumber")
    pdf <- pdfplumber$open(path)
    pages <- sapply(pdf$pages[1:min(2, length(pdf$pages))], function(p) {
      t <- p$extract_text()
      if (is.null(t)) "" else t
    })
    pdf$close()
    paste(pages, collapse = "\n")
  }
  tryCatch(
    if (!is.null(timeout)) R.utils::withTimeout(run(), timeout = timeout, onTimeout = "error") else run(),
    error = function(e) ""
  )
}

read_xlsx_text <- function(path, timeout = NULL) {
  run <- function() {
    sheet <- suppressMessages(readxl::read_excel(path, n_max = 10))
    paste(paste(names(sheet), collapse = " "),
          paste(apply(sheet, 1, paste, collapse = " "), collapse = " "))
  }
  tryCatch(
    if (!is.null(timeout)) R.utils::withTimeout(run(), timeout = timeout, onTimeout = "error") else run(),
    error = function(e) ""
  )
}

read_docx_text <- function(path, timeout = NULL) {
  run <- function() {
    doc <- officer::read_docx(path)
    content <- officer::docx_summary(doc)
    text_rows <- content[content$content_type == "paragraph", "text"]
    paste(text_rows, collapse = " ")
  }
  tryCatch(
    if (!is.null(timeout)) R.utils::withTimeout(run(), timeout = timeout, onTimeout = "error") else run(),
    error = function(e) ""
  )
}

read_txt_text <- function(path) {
  if (!file.exists(path)) return("")
  enc <- tryCatch(readr::guess_encoding(path)$encoding[1], error = function(e) "latin1")
  enc <- if (is.na(enc) || !nzchar(enc)) "latin1" else enc
  text <- tryCatch(
    paste(readLines(path, n = 50, encoding = enc, warn = FALSE), collapse = " "),
    error   = function(e) "",
    warning = function(w) tryCatch(
      paste(readLines(path, n = 50, encoding = "latin1", warn = FALSE), collapse = " "),
      error = function(e2) ""
    )
  )
  iconv(text, from = enc, to = "UTF-8", sub = "?")
}

extract_file_text <- function(path, max_chars = 300) {
  ext <- tolower(tools::file_ext(path))

  if (ext %in% c("doc", "dta", "jpg", "png", "rar", "sav", "zip", "xml", "xls")) {
    return("")
  }

  text <- switch(ext,
    pdf  = read_pdf_text(path, timeout = 120),
    xlsx = read_xlsx_text(path, timeout = 120),
    docx = read_docx_text(path, timeout = 120),
    read_txt_text(path)
  )

  substr(tryCatch(trimws(text), error = function(e) text), 1, max_chars)
}

# COMMAND ----------


