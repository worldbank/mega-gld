-- Databricks notebook source
-- TODO: Replace prd_mega.gld with prd_mega.sgld48 once the service principal provision is complete
-- The table names are built from job parameters, so that the staging job (see README, Staging
-- environment) writes <ouo_schema>.<table_prefix>gld_availability from
-- <target_schema>.<table_prefix>_ingestion_metadata. The widgets below hold the production
-- defaults and are overridden by the job parameters. The :param syntax needs Databricks Runtime 15.2 or newer.
CREATE WIDGET TEXT target_schema DEFAULT "prd_csc_mega.sgld48";

-- COMMAND ----------

CREATE WIDGET TEXT ouo_schema DEFAULT "prd_mega.sgld48";

-- COMMAND ----------

CREATE WIDGET TEXT table_prefix DEFAULT "";

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:ouo_schema || '.' || :table_prefix || 'gld_availability') (
    country STRING,
    year INT,
    survey STRING,
    classification STRING
)
USING DELTA;

-- COMMAND ----------

MERGE INTO IDENTIFIER(:ouo_schema || '.' || :table_prefix || 'gld_availability') AS target
USING (
    SELECT country, year, survey, classification
    FROM (
        SELECT
            country,
            year,
            survey,
            classification,
            ROW_NUMBER() OVER (PARTITION BY country, year, survey ORDER BY table_version DESC) AS rn
        FROM IDENTIFIER(:target_schema || '.' || :table_prefix || '_ingestion_metadata')
    )
    WHERE rn = 1
) AS source
ON target.country <=> source.country
   AND target.year <=> source.year
   AND target.survey <=> source.survey

WHEN MATCHED AND target.classification <=> source.classification = FALSE THEN
UPDATE SET target.classification = source.classification

WHEN NOT MATCHED THEN
INSERT (country, year, survey, classification)
VALUES (
    source.country,
    source.year,
    source.survey,
    source.classification
)

WHEN NOT MATCHED BY SOURCE THEN DELETE;
