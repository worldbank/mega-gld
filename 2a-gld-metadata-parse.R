library(purrr)
library(dplyr)
library(sparklyr)

source("helpers/config.R")
source("helpers/do_file_parsing.R")

sc <- spark_connect(method = "databricks")

metadata <- tbl(sc, METADATA_TABLE) %>% collect()

unpublished <- metadata %>%
  filter(published == FALSE,
    is.na(do_path)
  )

unpublished <- unpublished %>%
  mutate(do_path = map2_chr(dta_path, filename, find_do_files))

# --- parse the do files ---

parsed_rows <- lapply(seq_len(nrow(unpublished)), function(i) {
  row <- unpublished[i, ]
  do_file <- row$do_path[[1]]

  if (is.character(do_file) && file.exists(do_file)) {
    info <- parse_do_file(do_file)
  } else {
    info <- list()
  }

  info$filename <- row$filename
  info
})

parsed_df <- bind_rows(parsed_rows)

# --- compute version ---

v_cols <- grep("^V\\d{2}$", names(parsed_df), value = TRUE)

parsed_df$latest_v_text <- apply(parsed_df[v_cols], 1, function(x) {
  select_latest_version(as.list(x), v_cols)
})

parsed_df$version_label <- sapply(parsed_df$latest_v_text, make_version_label)

# --- add to metadata table ---

metadata <- metadata %>%
  mutate(
    version_label = coalesce(parsed_df$version_label[ match(filename, parsed_df$filename) ],version_label),
    do_path = coalesce(unpublished$do_path[ match(filename, unpublished$filename) ],do_path)
  )



copy_to(sc, metadata, "tmp_new_meta", overwrite = TRUE)

DBI::dbExecute(
  sc,
  paste0(
    "UPDATE ", METADATA_TABLE, " AS m ",
    "SET
      version_label = (
        SELECT ANY_VALUE(p.version_label)
        FROM tmp_new_meta p
        WHERE p.filename = m.filename
      ),
      do_path = (
        SELECT ANY_VALUE(p.do_path)
        FROM tmp_new_meta p
        WHERE p.filename = m.filename
      )
    WHERE m.filename IN (SELECT filename FROM tmp_new_meta)"
  )
)


DBI::dbExecute(sc, "DROP TABLE IF EXISTS tmp_new_meta")

