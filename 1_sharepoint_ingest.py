import os
import gc
import pandas as pd
from pyspark.sql import SparkSession

from helpers.ingestion_pipeline import (
    update_metadata,
    get_stata_var_labels,
    apply_column_comments,
    compute_stacking,
    get_table_version,
    table_exists,
)

def in_databricks() -> bool:
    return ("DATABRICKS_RUNTIME_VERSION" in os.environ)

if not in_databricks():
    print("Not running in Databricks. Skipping ingestion pipeline.")
else:
    %pip install pyreadstat
    spark = SparkSession.builder.getOrCreate()

    target_schema = "prd_csc_mega.sgld48"
    metadata_table = f"{target_schema}._ingestion_metadata"

    CHUNK_THRESHOLD_MB = 900
    CHUNK_SIZE = 5000

    # --- load metadata ---

    metadata = (
        spark.read.table(metadata_table)
        .select("dta_path","table_name", "country", "year", "survey", "quarter", "M_version", "A_version", "ingested")
        .toPandas()
    )

    candidates = metadata[metadata["ingested"] == False]

    if candidates.empty:
        print("No files to ingest.")
    else:
        # --- define ingestion order ---

        key_cols = ["country", "year", "survey", "quarter"]

        to_ingest = candidates.sort_values(
            key_cols + ["M_version", "A_version"],
            ascending=[True, True, True, True, True, True]
        )

        print("Files to ingest:", len(to_ingest))


        # --- ingest ----

        for _, row in to_ingest.iterrows():

            dta_path = row["dta_path"]
            tbl_name = str(row["table_name"]).strip()

            if not tbl_name or tbl_name.lower() == "nan":
                print(f"SKIPPING: missing table_name for dta_path={dta_path}")
                continue
            
            full_write_table = f"{target_schema}.{tbl_name}"    
            print("--- Processing:", dta_path)

            try:
                exists = table_exists(spark, full_write_table)
                if exists:
                    print("Table exists → overwriting")
                else:
                    print("Table does not exist → creating")
                
                size_mb = os.path.getsize(dta_path) / (1024 * 1024)
                
                if size_mb <= CHUNK_THRESHOLD_MB:

                    pdf = pd.read_stata(dta_path, convert_categoricals=False)

                    cols = pdf.columns

                    harm_type = (pdf["harmonization"].iloc[0] if "harmonization" in cols else None)
                    household_level = ("hhid" in cols and pdf["hhid"].notna().any())

                    sdf = spark.createDataFrame(pdf)

                    print("Writing:", full_write_table)
                    sdf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(full_write_table)

                    var_labels = get_stata_var_labels(dta_path)
                    #print("labels:", list(var_labels.items())[:10])
                    apply_column_comments(spark,full_write_table, var_labels)
                    table_version = get_table_version(spark,full_write_table)
                    update_metadata(spark, metadata_table,dta_path, tbl_name, harm_type, household_level, table_version)

                    del pdf, sdf
                    gc.collect()
                    continue

                print("Chunking:", full_write_table)

                reader = pd.read_stata(dta_path, chunksize=CHUNK_SIZE, iterator=True, convert_categoricals=False)

                first_chunk = next(reader)
                print("First chunk size:",first_chunk.memory_usage(deep=True).sum() / 1_048_576,"MB")

                cols = first_chunk.columns

                harm_type = (first_chunk["harmonization"].iloc[0] if "harmonization" in cols else None)
                household_level = ("hhid" in cols and first_chunk["hhid"].notna().any())

                var_labels = get_stata_var_labels(dta_path)

                #print("labels:", list(var_labels.items())[:10])

                spark_df = spark.createDataFrame(first_chunk)
                spark_schema = spark_df.schema

                print("Writing first chunk →", full_write_table)
                spark_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(full_write_table)

                apply_column_comments(spark,full_write_table, var_labels)

                del first_chunk
                del spark_df
                gc.collect()

                for i, chunk in enumerate(reader, start=2):
                    print(f"Chunk {i} size:", chunk.memory_usage(deep=True).sum() / 1_048_576, "MB")

                    spark_df = spark.createDataFrame(chunk, schema=spark_schema)
                    spark_df.write.mode("append").format("delta").saveAsTable(full_write_table)

                    del chunk
                    del spark_df
                    gc.collect()

                # --- update metadata ---
                table_version = get_table_version(spark,full_write_table)
                update_metadata(spark, metadata_table,dta_path, tbl_name, harm_type, household_level, table_version)
            
            except Exception as e:
                print(f"ERROR ingesting {dta_path}: {e}")

            for obj in ["reader", "pdf", "first_chunk", "chunk", "spark_df", "spark_schema"]:
                if obj in locals():
                    del locals()[obj]
            gc.collect()


        # --- update stacking for all ingested tables ---

        metadata = spark.read.table(metadata_table).toPandas()
        metadata = metadata[metadata["ingested"] == True].copy()

        metadata = compute_stacking(metadata)

        updates = metadata[["dta_path", "stacking"]].drop_duplicates()

        spark.createDataFrame(updates).createOrReplaceTempView("stacking_updates")

        spark.sql(f"""
        MERGE INTO {metadata_table} t
        USING stacking_updates s
        ON t.dta_path = s.dta_path
        WHEN MATCHED THEN UPDATE SET t.stacking = s.stacking
        """)

        print("Done: stacking updated.")



