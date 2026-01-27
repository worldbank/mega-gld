library(testthat)

source(file.path(dirname(dirname(getwd())), "helpers", "do_file_parsing.R"))

# --- Tests for find_do_files ---
# Structure: base/dataset_folder/version_folder/Data/Harmonized
# Programs folder is at: base/dataset_folder/Programs

test_that("find_do_files returns single .do file when only one exists", {
  tmp <- tempdir()
  base_dir <- file.path(tmp, "test_base")
  dataset_dir <- file.path(base_dir, "USA_2020_LFS")
  version_dir <- file.path(dataset_dir, "v01")
  programs_dir <- file.path(dataset_dir, "Programs")
  harmonized_path <- file.path(version_dir, "Data", "Harmonized")

  dir.create(programs_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(harmonized_path, recursive = TRUE, showWarnings = FALSE)
  file.create(file.path(programs_dir, "script.do"))

  result <- find_do_files(harmonized_path, "test_dataset")

  expect_true(grepl("script\\.do$", result))

  unlink(base_dir, recursive = TRUE)
})

test_that("find_do_files returns empty string when Programs dir missing", {
  tmp <- tempdir()
  base_dir <- file.path(tmp, "test_base_no_prog")
  dataset_dir <- file.path(base_dir, "USA_2020_LFS")
  version_dir <- file.path(dataset_dir, "v01")
  harmonized_path <- file.path(version_dir, "Data", "Harmonized")

  dir.create(harmonized_path, recursive = TRUE, showWarnings = FALSE)

  result <- find_do_files(harmonized_path, "test_dataset")

  expect_equal(result, "")

  unlink(base_dir, recursive = TRUE)
})

test_that("find_do_files prefers file matching filename when two .do files exist", {
  tmp <- tempdir()
  base_dir <- file.path(tmp, "test_base_two")
  dataset_dir <- file.path(base_dir, "USA_2020_LFS")
  version_dir <- file.path(dataset_dir, "v01")
  programs_dir <- file.path(dataset_dir, "Programs")
  harmonized_path <- file.path(version_dir, "Data", "Harmonized")

  dir.create(programs_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(harmonized_path, recursive = TRUE, showWarnings = FALSE)
  file.create(file.path(programs_dir, "USA_2020_LFS.do"))
  file.create(file.path(programs_dir, "other.do"))

  result <- find_do_files(harmonized_path, "USA_2020_LFS")

  expect_true(grepl("USA_2020_LFS\\.do$", result))

  unlink(base_dir, recursive = TRUE)
})

test_that("find_do_files prefers file named *ALL.do when two .do files exist and none matches the filename", {
  tmp <- tempdir()
  base_dir <- file.path(tmp, "test_base_two")
  dataset_dir <- file.path(base_dir, "USA_2020_LFS")
  version_dir <- file.path(dataset_dir, "v01")
  programs_dir <- file.path(dataset_dir, "Programs")
  harmonized_path <- file.path(version_dir, "Data", "Harmonized")

  dir.create(programs_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(harmonized_path, recursive = TRUE, showWarnings = FALSE)
  file.create(file.path(programs_dir, "prefix_ALL.do"))
  file.create(file.path(programs_dir, "other.do"))

  result <- find_do_files(harmonized_path, "USA_2020_LFS")

  expect_true(grepl("prefix_ALL\\.do$", result))

  unlink(base_dir, recursive = TRUE)
})

# --- Tests for extract_version_control ---

test_that("extract_version_control parses proper start-end block with bullet points", {
  text <- "
Some header text
<_Version Control_>
* 2023-01-15 - Initial version
* 2023-02-20 - Added new variables
* 2023-03-10 - Fixed coding errors
</_Version Control_>
Some footer text
"
  result <- extract_version_control(text)

  expect_true(length(result) >= 3)
  expect_true(any(grepl("Initial version", unlist(result))))
  expect_true(any(grepl("Added new variables", unlist(result))))
  expect_true(any(grepl("Fixed coding errors", unlist(result))))
  expect_false(any(grepl("version control", unlist(result), ignore.case = TRUE)))
  expect_false(any(grepl("<", unlist(result))))
})

test_that("extract_version_control handles missing start tag with end tag", {
  text <- "* 2023-01-15 - First change
* 2023-02-20 - Second change
</_Version Control_>"
  result <- extract_version_control(text)

  expect_true(length(result) >= 2)
  expect_true(any(grepl("First change", unlist(result))))
  expect_true(any(grepl("Second change", unlist(result))))
  expect_false(any(grepl("version control", unlist(result), ignore.case = TRUE)))
})

test_that("extract_version_control handles missing end tag with start tag", {
  text <- "<_Version Control_>
* 2023-01-15 - Only change
Some other content"
  result <- extract_version_control(text)

  expect_equal(length(result), 1)
  expect_true(grepl("Only change", result$V01))
  expect_false(any(grepl("version control", unlist(result), ignore.case = TRUE)))
})

test_that("extract_version_control returns empty list when no tags present", {
  text <- "This is just some random text without any version control tags"
  result <- extract_version_control(text)

  expect_equal(length(result), 0)
  expect_type(result, "list")
})

test_that("extract_version_control handles multi-line bullet entries", {
  text <- "<_Version Control_>
* 2023-01-15 - This is a long description
  that continues on the next line
* 2023-02-20 - Short one
</_Version Control_>"
  result <- extract_version_control(text)

  expect_true(length(result) >= 2)
  expect_true(any(grepl("continues on the next line", unlist(result))))
  expect_false(any(grepl("version control", unlist(result), ignore.case = TRUE)))
  expect_false(any(grepl("<", unlist(result))))
})

# --- Tests for extract_v_text ---

test_that("extract_v_text removes Date: prefix", {
  raw <- "Date: 2023-01-15 - Added new feature"
  result <- extract_v_text(raw)
  expect_equal(result, "Added new feature")
})

test_that("extract_v_text removes File: prefix", {
  raw <- "File: some description here"
  result <- extract_v_text(raw)
  expect_equal(result, "some description here")
})

test_that("extract_v_text removes surrounding brackets", {
  raw <- "[Some text in brackets]"
  result <- extract_v_text(raw)
  expect_equal(result, "Some text in brackets")
})

test_that("extract_v_text translates Chinese punctuation", {
  raw <- "Test；with：Chinese，punctuation"
  result <- extract_v_text(raw)
  expect_equal(result, "Test;with:Chinese,punctuation")
})

test_that("extract_v_text returns empty string for NA input", {
  expect_equal(extract_v_text(NA), "")
})

test_that("extract_v_text normalizes whitespace", {
  raw <- "Text   with    multiple     spaces"
  result <- extract_v_text(raw)
  expect_equal(result, "Text with multiple spaces")
})

# --- Tests for select_latest_version ---

test_that("select_latest_version returns last non-empty version", {
  row <- list(V01 = "First version", V02 = "Second version", V03 = "Third version")
  v_cols <- c("V01", "V02", "V03")
  result <- select_latest_version(row, v_cols)
  expect_equal(result, "Third version")
})

test_that("select_latest_version skips NA values", {
  row <- list(V01 = "First version", V02 = "Second version", V03 = NA)
  v_cols <- c("V01", "V02", "V03")
  result <- select_latest_version(row, v_cols)
  expect_equal(result, "Second version")
})

test_that("select_latest_version skips empty strings", {
  row <- list(V01 = "First version", V02 = "", V03 = "   ")
  v_cols <- c("V01", "V02", "V03")
  result <- select_latest_version(row, v_cols)
  expect_equal(result, "First version")
})

test_that("select_latest_version skips 'description of changes' placeholder", {
  row <- list(V01 = "Real content", V02 = "Description of changes")
  v_cols <- c("V01", "V02")
  result <- select_latest_version(row, v_cols)
  expect_equal(result, "Real content")
})

test_that("select_latest_version returns empty string when all empty", {
  row <- list(V01 = NA, V02 = "", V03 = "   ")
  v_cols <- c("V01", "V02", "V03")
  result <- select_latest_version(row, v_cols)
  expect_equal(result, "")
})

# --- Tests for make_version_label ---

test_that("make_version_label trims whitespace", {
  expect_equal(make_version_label("  some text  "), "some text")
})

test_that("make_version_label returns empty for empty input", {
  expect_equal(make_version_label(""), "")
  expect_equal(make_version_label("   "), "")
})

test_that("make_version_label returns empty for non-character input", {
  expect_equal(make_version_label(123), "")
  expect_equal(make_version_label(NULL), "")
})
