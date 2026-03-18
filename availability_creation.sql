-- Databricks notebook source
-- TODO: Replace prd_mega.gld with prd_mega.sgld48 once the service principal provision is complete
CREATE TABLE IF NOT EXISTS prd_mega.gld.gld_availability (
    country STRING,
    year INT,
    survey STRING,
    classification STRING
)
USING DELTA;

-- COMMAND ----------

MERGE INTO prd_mega.gld.gld_availability AS target
USING (
    SELECT DISTINCT
        country,
        year,
        survey,
        classification
    FROM prd_csc_mega.sgld48._ingestion_metadata
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