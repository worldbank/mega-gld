-- Databricks notebook source
-- This notebook is not part of the main pipeline. It creates a small sample of the _ingestion_metadata table, which can be used to try out incremental_sync on a few surveys before running it on all of them (see tools/stacking_test_run). Production tables are only read, never modified.

-- COMMAND ----------

-- 1. CANDIDATES: all tables flagged for stacking, with the dynamic columns (subnatid*, gaul_adm*_code) each one contains
SELECT
  m.table_name,
  m.country,
  m.year,
  m.survey,
  m.quarter,
  m.classification,
  m.stacked_all_table_version,
  m.stacked_ouo_table_version,
  COUNT(c.column_name) AS n_dynamic_cols,
  ARRAY_SORT(COLLECT_LIST(c.column_name)) AS dynamic_cols
FROM prd_csc_mega.sgld48._ingestion_metadata m
LEFT JOIN prd_csc_mega.information_schema.columns c
  ON c.table_schema = 'sgld48'
  AND c.table_name = LOWER(m.table_name)
  AND (c.column_name RLIKE '^subnatid[0-9]+(_prev)?$' OR c.column_name RLIKE '^gaul_adm[0-9]+_code$')
WHERE m.stacking = 1
GROUP BY ALL
ORDER BY n_dynamic_cols DESC, m.table_name

-- COMMAND ----------

-- 2. SAMPLE: copy the metadata of a few surveys to _ingestion_metadata_test.
-- For each classification, and separately for annual and quarterly surveys, it takes the two tables with the most dynamic columns and the one with the fewest.
-- To hand-pick the surveys instead, replace the "picked" subquery with:
--   SELECT * FROM prd_csc_mega.sgld48._ingestion_metadata WHERE table_name IN ('table_1', 'table_2')
CREATE OR REPLACE TABLE prd_csc_mega.sgld48._ingestion_metadata_test AS
WITH dynamic_cols AS (
  SELECT table_name, COUNT(*) AS n_dynamic_cols
  FROM prd_csc_mega.information_schema.columns
  WHERE table_schema = 'sgld48'
    AND (column_name RLIKE '^subnatid[0-9]+(_prev)?$' OR column_name RLIKE '^gaul_adm[0-9]+_code$')
  GROUP BY table_name
),
ranked AS (
  SELECT
    m.country,
    m.year,
    m.survey,
    m.quarter,
    ROW_NUMBER() OVER (
      PARTITION BY m.classification, m.quarter = 'NA'
      ORDER BY COALESCE(d.n_dynamic_cols, 0) DESC, m.table_name
    ) AS most_dynamic,
    ROW_NUMBER() OVER (
      PARTITION BY m.classification, m.quarter = 'NA'
      ORDER BY COALESCE(d.n_dynamic_cols, 0), m.table_name
    ) AS fewest_dynamic
  FROM prd_csc_mega.sgld48._ingestion_metadata m
  LEFT JOIN dynamic_cols d
    ON d.table_name = LOWER(m.table_name)
  WHERE m.stacking = 1
)
SELECT m.*
FROM prd_csc_mega.sgld48._ingestion_metadata m
LEFT SEMI JOIN (
  SELECT * FROM ranked WHERE most_dynamic <= 2 OR fewest_dynamic = 1
) picked
  ON m.country = picked.country
  AND m.year = picked.year
  AND m.survey = picked.survey
  AND m.quarter = picked.quarter

-- COMMAND ----------

-- The sample (all versions of the picked surveys are copied, only the ones with stacking = 1 get stacked)
SELECT table_name, filename, classification, stacking, table_version, stacked_all_table_version, stacked_ouo_table_version
FROM prd_csc_mega.sgld48._ingestion_metadata_test
ORDER BY table_name, filename

-- COMMAND ----------

-- 3. FIRST RUN: flag all the surveys of the sample as not stacked yet, and drop the harmonized test tables left by a previous test (incremental_sync re-creates them).
-- Then run tools/stacking_test_run: all the surveys of the sample get stacked.
UPDATE prd_csc_mega.sgld48._ingestion_metadata_test
SET stacked_all_table_version = NULL,
    stacked_ouo_table_version = NULL

-- COMMAND ----------

DROP TABLE IF EXISTS prd_csc_mega.sgld48.gld_harmonized_all_test

-- COMMAND ----------

DROP TABLE IF EXISTS prd_csc_mega.sgld48.gld_harmonized_ouo_test

-- COMMAND ----------

-- 4. SECOND RUN: flag one survey of the sample as not stacked yet, then run tools/stacking_test_run again.
-- Only that survey is re-stacked: the dynamic columns of all the other ones must be preserved.
-- The best pick is the table with the fewest dynamic columns (last row of the candidates query, restricted to the sample).
UPDATE prd_csc_mega.sgld48._ingestion_metadata_test
SET stacked_all_table_version = NULL,
    stacked_ouo_table_version = NULL
WHERE table_name = 'table_1'

-- COMMAND ----------

-- 5. RESULTS: rows per survey in the harmonized test tables
SELECT 'all' AS harmonized, countrycode, year, survname, quarter, COUNT(*) AS n_rows
FROM prd_csc_mega.sgld48.gld_harmonized_all_test
GROUP BY ALL
UNION ALL
SELECT 'ouo' AS harmonized, countrycode, year, survname, quarter, COUNT(*) AS n_rows
FROM prd_csc_mega.sgld48.gld_harmonized_ouo_test
GROUP BY ALL
ORDER BY harmonized, countrycode, year, survname, quarter

-- COMMAND ----------

-- Stacked versions recorded in the test metadata, to be compared with the latest versions of the harmonized test tables
SELECT table_name, classification, stacked_all_table_version, stacked_ouo_table_version
FROM prd_csc_mega.sgld48._ingestion_metadata_test
WHERE stacking = 1
ORDER BY table_name

-- COMMAND ----------

DESCRIBE HISTORY prd_csc_mega.sgld48.gld_harmonized_all_test

-- COMMAND ----------

DESCRIBE HISTORY prd_csc_mega.sgld48.gld_harmonized_ouo_test

-- COMMAND ----------

-- 6. CLEAN UP: drop the three test tables once the test is over
-- DROP TABLE IF EXISTS prd_csc_mega.sgld48._ingestion_metadata_test

-- COMMAND ----------

-- DROP TABLE IF EXISTS prd_csc_mega.sgld48.gld_harmonized_all_test

-- COMMAND ----------

-- DROP TABLE IF EXISTS prd_csc_mega.sgld48.gld_harmonized_ouo_test
