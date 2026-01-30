import pyreadstat
from pyspark.sql.functions import col

def update_metadata(spark, metadata_table, dta_path, tbl_name, harm_type, household_level, table_version):
    harm_sql = "NULL" if harm_type is None else f"'{harm_type}'"
    hh_sql = "NULL" if household_level is None else ("TRUE" if household_level else "FALSE")
    tv_sql = "NULL" if table_version is None else str(int(table_version))

    spark.sql(f"""
        UPDATE {metadata_table}
        SET
            ingested = TRUE,
            harmonization = {harm_sql},
            household_level = {hh_sql},
            table_name = '{tbl_name}',
            table_version = {tv_sql}
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

            safe_label = (str(label)
                .replace("\x00", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .strip()
                .replace("'", "")
            )

            spark.sql(
                f"ALTER TABLE {full_table_name} "
                f"ALTER COLUMN `{col_name}` "
                f"COMMENT '{safe_label}'"
            )

        except Exception as e:
            print(f"Skipping comment for {full_table_name}.{col_name}: {e}")


def compute_stacking(pdf):
    out = pdf.copy()

    # default: stacking = 1
    out["stacking"] = 1

    # add 0 for panel tables
    is_panel = out["table_name"].astype(str).str.contains("panel", case=False, na=False)
    out.loc[is_panel, "stacking"] = 0

    # annual rows only (quarter null/empty) can conflict on country-year
    is_annual = out["quarter"].isna() | (out["quarter"].astype(str).str.strip() == "")
    annual = out[is_annual]

    # duplicates among annual (country, year)
    conflict_mask = annual.duplicated(subset=["country", "year"], keep=False)

    if conflict_mask.any():
        conflicted = annual[conflict_mask].copy()

        # priority: GLD over GLD-Light
        harm_prio = (
            conflicted["harmonization"]
            .map({"GLD": 2, "GLD-Light": 1})
            .fillna(0)
            .astype(int)
        )

        conflicted["harm_prio"] = harm_prio

        # best first
        conflicted = conflicted.sort_values(
            ["country", "year", "harm_prio", "M_version", "A_version"],
            ascending=[True, True, False, False, False],
        )

        # pick winner per (country, year)
        winners = conflicted.drop_duplicates(
            subset=["country", "year"], keep="first"
        ).index

        # set all conflicted annual rows to 0, then winners to 1
        out.loc[conflicted.index, "stacking"] = 0
        out.loc[winners, "stacking"] = 1

        # panel always stays 0
        out.loc[is_panel, "stacking"] = 0

    return out


def get_table_version(spark, full_table_name):
    hist = spark.sql(f"DESCRIBE HISTORY {full_table_name}")
    return int(hist.select("version").orderBy(col("version").desc()).first()["version"])


def table_exists(spark, full_table_name):
    return spark.catalog.tableExists(full_table_name)