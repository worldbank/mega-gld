import pyreadstat
from pyspark.sql.functions import col

def sql_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        v = (
            v.replace("\x00", " ")
             .replace("\r", " ")
             .replace("\n", " ")
             .strip()
             .replace("'", "")
        )
        return f"'{v}'" 
    raise TypeError(f"Unsupported type: {type(v)}")

def update_metadata(spark, metadata_table, dta_path, tbl_name, harm_type, household_level, table_version):
    set_parts = [
        "ingested = TRUE",
        f"harmonization = {sql_literal(harm_type)}",
        f"household_level = {sql_literal(household_level)}",
        f"table_name = {sql_literal(tbl_name)}",
        f"table_version = {sql_literal(table_version)}",
    ]

    spark.sql(f"""
        UPDATE {metadata_table}
        SET {", ".join(set_parts)}
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


def apply_column_comments(spark, full_table_name, var_labels):
    for col_name, label in var_labels.items():
        try:
            if label is None:
                continue

            safe_label = sql_literal(str(label))

            spark.sql(
                f"ALTER TABLE {full_table_name} "
                f"ALTER COLUMN `{col_name}` "
                f"COMMENT {safe_label}"
            )

        except Exception as e:
            print(f"Skipping comment for {full_table_name}.{col_name}: {e}")




def get_table_version(spark, full_table_name):
    hist = spark.sql(f"DESCRIBE HISTORY {full_table_name}")
    return int(hist.select("version").orderBy(col("version").desc()).first()["version"])


def table_exists(spark, full_table_name):
    return spark.catalog.tableExists(full_table_name)