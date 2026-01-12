import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first
from pyspark.sql.utils import AnalysisException
import gc
%pip install pyreadstat
import pyreadstat


spark = SparkSession.builder.getOrCreate()

metadata_table = "prd_csc_mega.sgld48._ingestion_metadata"
target_schema  = "prd_csc_mega.sgld48"

CHUNK_THRESHOLD_MB = 900
CHUNK_SIZE = 5000

# --- load metadata ---

metadata = (
    spark.read.table(metadata_table)
    .filter(col("ingested") == False)
    .select("dta_path")
    .toPandas()
)

if metadata.empty:
    print("No files to ingest.")
    raise SystemExit


print("Files to ingest:", len(metadata))


# --- helper functions ---
def make_table_name(path):
    nm = os.path.basename(path).replace(".dta", "")
    nm = "".join(c if c.isalnum() else "_" for c in nm)
    while "__" in nm:
        nm = nm.replace("__", "_")
    return nm


def update_metadata(dta_path, tbl_name, harm_type, household_level):
    harm_sql = "NULL" if harm_type is None else f"'{harm_type}'"
    hh_sql = "NULL" if household_level is None else ("TRUE" if household_level else "FALSE")

    spark.sql(f"""
        UPDATE {metadata_table}
        SET
            ingested = TRUE,
            harmonization = {harm_sql},
            household_level = {hh_sql},
            table_name = '{tbl_name}'
        WHERE dta_path = '{dta_path}'
    """)

    print("Updated metadata for:", dta_path)

def get_stata_var_labels(dta_path):
    _, meta = pyreadstat.read_dta(dta_path, metadataonly=True)
    return {
        name: label
        for name, label in zip(meta.column_names, meta.column_labels)
        if label is not None and str(label).strip() != ""
    }

def apply_column_comments(full_table_name, var_labels):
    for col_name, label in var_labels.items():
        safe_label = label.replace("'", "''")
        safe_col = col_name.replace("`", "``")
        spark.sql(f"""
            ALTER TABLE {full_table_name}
            ALTER COLUMN `{col_name}`
            COMMENT '{safe_label}'
        """)

# --- ingest ----

for dta_path in metadata["dta_path"]:
    print("--- Processing:", dta_path)

    try:

        tbl_name = make_table_name(dta_path)
        full_table_name = f"{target_schema}.{tbl_name}"

        tbl_exists = (
            spark.catalog.listTables(target_schema)
        )
        exists = any(t.name == tbl_name for t in tbl_exists)

        if exists:
            print("Table exists. Skipping")
            continue

        size_mb = os.path.getsize(dta_path) / (1024 * 1024)
        if size_mb <= CHUNK_THRESHOLD_MB:

            pdf = pd.read_stata(dta_path, convert_categoricals=False)

            cols = pdf.columns

            harm_type = (
                pdf["harmonization"].iloc[0]
                if "harmonization" in cols else None
            )

            household_level = ("hhid" in cols and pdf["hhid"].notna().any())

            sdf = spark.createDataFrame(pdf)

            print("Writing:", full_table_name)
            sdf.write.mode("overwrite").format("delta").saveAsTable(full_table_name)

            var_labels = get_stata_var_labels(dta_path)
            print("labels:", list(var_labels.items())[:10])
            apply_column_comments(full_table_name, var_labels)

            update_metadata(dta_path, tbl_name, harm_type, household_level)
            continue

        print("Chunking:", full_table_name)

        reader = pd.read_stata(dta_path, chunksize=CHUNK_SIZE, iterator=True, convert_categoricals=False)

        first_chunk = next(reader)

        var_labels = get_stata_var_labels(dta_path)
        print("labels:", list(var_labels.items())[:10])

        print("First chunk size:",
            first_chunk.memory_usage(deep=True).sum() / 1_048_576,
            "MB")

        spark_df = spark.createDataFrame(first_chunk)
        spark_schema = spark_df.schema

        cols = first_chunk.columns

        harm_type = (
            first_chunk["harmonization"].iloc[0]
            if "harmonization" in cols else None
        )

        household_level = ("hhid" in cols and first_chunk["hhid"].notna().any())

        print("Writing first chunk →", full_table_name)
        spark_df.write.mode("overwrite").format("delta") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)

        apply_column_comments(full_table_name, var_labels)

        del first_chunk
        del spark_df
        gc.collect()

        for i, chunk in enumerate(reader, start=2):
            print(f"Chunk {i} size:",
                chunk.memory_usage(deep=True).sum() / 1_048_576,
                "MB")

            spark_df = spark.createDataFrame(chunk, schema=spark_schema)
            spark_df.write.mode("append").format("delta").saveAsTable(full_table_name)

            del chunk
            del spark_df
            gc.collect()


        # --- update metadata ---
        update_metadata(dta_path, tbl_name, harm_type, household_level)
    
    except Exception as e:
        print(f"ERROR ingesting {dta_path}: {e}")

    for obj in ["reader", "pdf", "first_chunk", "chunk", "reader", "spark_df", "spark_schema"]:
        if obj in locals():
            del locals()[obj]
    gc.collect()


print("Ingestion complete.")



