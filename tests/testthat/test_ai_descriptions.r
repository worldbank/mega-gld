# Databricks notebook source
suppressPackageStartupMessages({
    library(testthat)
    library(fs)
})

# COMMAND ----------

# MAGIC %run "../../helpers/ai_description"

# COMMAND ----------

if (!exists("get_azure_openai_token")) {
  repo_root <- normalizePath(file.path("..", ".."), mustWork = TRUE)
  withr::local_dir(repo_root)
  source(file.path(repo_root, "helpers", "ai_description.r"))
}

# COMMAND ----------

write_txt <- function(path, content, encoding = "UTF-8") {
  con <- file(path, open = "w", encoding = encoding)
  writeLines(content, con)
  close(con)
}

# COMMAND ----------


# --- read_txt_text ---

test_that("read_txt_text returns empty string for nonexistent file", {
  expect_equal(read_txt_text("/nonexistent/path/file.txt"), "")
})

test_that("read_txt_text reads UTF-8 text correctly", {
  tmp <- tempfile(fileext = ".txt")
  writeLines("Hello world this is a plain text file for testing extraction", tmp)
  out <- read_txt_text(tmp)
  expect_true(nchar(trimws(out)) > 0)
  expect_true(grepl("Hello world", out))
  unlink(tmp)
})

test_that("read_txt_text reads latin1 text without erroring", {
  tmp <- tempfile(fileext = ".txt")
  write_txt(tmp, "Pesquisa Nacional: informações gerais sobre domicílios", encoding = "latin1")
  out <- read_txt_text(tmp)
  expect_true(nchar(trimws(out)) > 0)
  unlink(tmp)
})

# --- read_docx_text ---

test_that("read_docx_text reads docx content", {
  skip_if_not_installed("officer")
  tmp <- tempfile(fileext = ".docx")
  doc <- officer::read_docx()
  doc <- officer::body_add_par(doc, "This is a test paragraph for docx extraction.")
  print(doc, target = tmp)
  out <- read_docx_text(tmp)
  expect_true(grepl("test paragraph", out))
  unlink(tmp)
})

test_that("read_docx_text returns empty string for unreadable file", {
  out <- read_docx_text("/nonexistent/path/file.docx")
  expect_equal(out, "")
})

# --- read_xlsx_text ---

test_that("read_xlsx_text reads xlsx content", {
  skip_if_not_installed("readxl")
  skip_if_not_installed("writexl")
  tmp <- tempfile(fileext = ".xlsx")
  writexl::write_xlsx(data.frame(country = "Brazil", year = 2020), tmp)
  out <- read_xlsx_text(tmp)
  expect_true(grepl("country|Brazil", out))
  unlink(tmp)
})

test_that("read_xlsx_text returns empty string for unreadable file", {
  out <- read_xlsx_text("/nonexistent/path/file.xlsx")
  expect_equal(out, "")
})

# --- extract_file_text ---

test_that("extract_file_text returns empty string for skipped extensions", {
  for (ext in c("doc", "dta", "jpg", "png", "rar", "sav", "zip", "xml", "xls")) {
    tmp <- tempfile(fileext = paste0(".", ext))
    file.create(tmp)
    expect_equal(extract_file_text(tmp), "")
    unlink(tmp)
  }
})

test_that("extract_file_text returns empty string for unreadable file", {
  expect_equal(extract_file_text("/nonexistent/path/file.txt"), "")
})

test_that("extract_file_text returns empty string for empty file", {
  tmp <- tempfile(fileext = ".txt")
  file.create(tmp)
  expect_equal(trimws(extract_file_text(tmp)), "")
  unlink(tmp)
})

test_that("extract_file_text truncates output to max_chars", {
  tmp <- tempfile(fileext = ".txt")
  writeLines(paste(rep("a", 1000), collapse = ""), tmp)
  out <- extract_file_text(tmp, max_chars = 100)
  expect_lte(nchar(out), 100)
  unlink(tmp)
})



# COMMAND ----------

tmp <- tempfile(fileext = ".docx")
doc <- officer::read_docx()
doc <- officer::body_add_par(doc, "This is a test paragraph for docx extraction.")
print(doc, target = tmp)
read_docx_text(tmp)
