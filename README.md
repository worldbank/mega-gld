# mega-gld

[![Run Tests](https://github.com/dime-worldbank/mega-gld/actions/workflows/test.yml/badge.svg)](https://github.com/dime-worldbank/mega-gld/actions/workflows/test.yml)

#### Automation pipeline to onboard the Global Labor Database to the Corporate Data Lake and publish its datasets to the Microdata Library
#### 
The pipeline is organized in three different jobs:
1. Ingestion and Stacking (daily) [ADD LINK]
2. Publication of individual tables to Microdata Library (monthly) [ADD LINK]
3. Publication of stacked tables to Microdata Library (monthly) [ADD LINK]

This is a list of all support folders/files/tables:
* The **`_ingestion_metadata table`** is a table that keeps track of which tables have been ingested and published in the Microdata Library, as well as of some dataset-specific metadata. It also stores the path to the dta files and do files that have been associated with that specific ingestion
* [This Sharepoint file](https://worldbankgroup.sharepoint.com/:x:/r/sites/dap/gld/_layouts/15/Doc.aspx?sourcedoc=%7B3F8DB34F-8359-4BFC-86A0-5D14776E59E7%7D&file=survey-metadata.xlsx&action=default&mobileredirect=true){:target="_blank"} keeps track of all country survey specific metadata. 
> When a new survey is onboarded to the GLD catalog, the survey should be added in this csv file. 
* Country codes and names are matched using the MEGA table at prd_mega.indicator.country. 
* The folder /Volumes/prd_csc_mega/sgld48/vgld48/Workspace/**json_to_publish**/ contains all json files awaiting publications. 
> These files are created in the ***_json_creation** scripts and deleted once publication is successfully completed in the ***_json_publication** scripts
> Please note that Databricks tables cannot include the character "-" in their name. Therefore, "-" is replaced with "_" 
<br>
<br>

## Scripts
### Ingestion and Stacking Job Scripts
___
**delta_identification:** identifies new tables that need to be ingested. To do so, it crawls the entire Sharepoint library to identify "Data/Harmonized" folders.The new identified tables are added to the __ingestion_metadata_ table. 
This script features the creation of the following __ingestion_metadata_ variables: `filename`, `dta_path`, `country`, `year`, `quarter`, `survey`, `M_version`, `A_version`. These are  metadata fields that can be inferred from the filename. `table_name` is also computed, by removing M_version and A_version from the filename (since each table version is rewritten to the same table)
___
**sharepoint_ingestion:** ingests all new tables to prd_csc_mega.sgld48, either fully or in 5000-line chunks, if the file exceeds 900MB. The script also saves all variable labels as column comments in Databricks. At the end of the ingestion process, the following variables are updated in __ingestion_metadata_ 
- `harmonization` - GLD or GLD Light; 
- `household_level` - TRUE if the dataset contains the hhid variable; 
- `ingested` - TRUE if the ingestion process is successful
- `table_version` - After each table is fully written, the script retrieves the table’s latest Delta version (the maximum version number). This version is stored in the __ingestion_metadata_ table and used for reproducibility, since the tables are overwritten when new versions are ingested.

___
**metadata_parsing:** This scripts populates metadata fields that cannot be inferred from the filename:
1. It identifies the .do file that corresponds to the .dta file ingested, and parses it to extract the description of changes from the previous version to the current one. Both `do_path` and `version_label` are stored in the __ingestion_metadata_ table.
2. It identifies the .txt file "Where is this data from? - Readme.txt" file that corresponds to the .dta file ingested, and parses it to extract the data classification. This is stored as `classification` in the __ingestion_metadata__ table.
It also computes the `stacking` flag (1 if the table is supposed to be stacked in the _gld_harmonized_*_ tables, 0 otherwise).
> The stacking flag identifies which dataset versions should be included for each country–year combination. Only datasets for which the data classification was successfully parsed are eligible to be stacked. For both annual data (by country–year) and quarterly data (by country–year–quarter), the most recent eligible version is stacked; if the latest version does not have a data classification in the __ingestion_metadata_ table, the logic falls back to the next most recent classified version. Panel datasets are always excluded from stacking, all other rows default to stacking = 0, and when multiple harmonization types exist for the same country–year, GLD datasets are preferred over GLD-Light.
___
**table_stacking:** forthcoming.
Every time a specific version of a data table is added to a stacked table (either as a new country/year addition or replacing an older version of a country/year dataset), the databricks table version of the stacked table is recorded in the __ingestion_metadata_ table.
> Example:
> THA_2021_LFS-Q2_V01_M_V02_A_GLD is ingested in tha_2021_lfs_q2 table, overwriting its content (i.e.THA_2021_LFS-Q2_V01_M_V01_A_GLD). Subsequently, the rows of the _gld_harmonized_*_ table for Thailand 2021 are dropped, and replaced with the new rows from tha_2021_lfs_q2. After the replacement has occurred, the script retrieves the  _gld_harmonized_*_ latest Delta version (the maximum version number). This version is stored in the __ingestion_metadata_ table in the `stacked_ouo_table_version` column and used to compute the version statement in the **harmonized_json_creation** script.
<br>
<br>



### Publication of individual tables to Microdata Library Job Scripts
___
**json_creation:** creates all the .json files from metadata in the __ingestion_metadata_ table, using the supporting files _countries.csv_, and _survey_metadata.csv_, and saves them in the json_temp folder. It also computes and verifies github links for those surveys that have documentation published in github.
___
**json_publication:** publishes all the json files in json_temp, alongside the corresponding datasets (using the file in the `dta_path` column of the __ingestion_metadata_ table), .do files (using the file in the `do_path` column of the __ingestion_metadata_ table) and technical documentation/questionnaires (found in the corresponding Docs folder). 
> Please note that the environment variable "NADA_API_KEY" is used to publish. if the API key needs to be updated, follow [these instructions](https://docs.databricks.com/aws/en/security/secrets/). The scope is GLDKEYVAULT, and it can be managed by anyone in the ITSDA-LKHS-DAP-PROD-gld team.
> Please note that the structure of the Docs folders varies. Some Docs folders contain files, some contain subfolders called Technical and Questionnaires - however these can be empty. For convenience, if documents are correctly organized, they will be published as two separate resources (Questionnaires and Technical Documentation). If they are not, all documents get uploaded as Technical Documentation. 
<br>
<br>


### Publication of harmonized/stacked tables to Microdata Library Job Scripts
___
**harmonized_json_creation:** computes necessary metadata fields and creates the .json files for the _gld_harmonized_*_ tables. 

> Publication to the Microdata Library requires explicit versioning. The **harmonized_json_publication** script assigns a version identifier (V*) to each _gld_harmonized_* table every time it is published. Each published version must include a version statement describing all changes made since the previous publication. This version statement is constructed in **harmonized_json_creation.** For example, consider publishing version V2 of the __gld_harmonized_all_ table. Suppose the current Databricks table version of __gld_harmonized_all_ is 15. To identify the Databricks table version corresponding to the previously published version (V1), the script queries the __ingestion_metadata_ table and retrieves the value of `stacked_all_published_table_version` for rows where `stacked_all_published_version` = 1. In this example, the returned value is 10, indicating that Databricks table version 10 corresponds to the published V1. The script then scans the __ingestion_metadata_ table for all country–year combinations with `stacked_all_published_table_version` > 10 and compiles the version statement accordingly, for example: “Updated data for [Country] [Year], [Country] [Year], …”.
___
**harmonized_json_publication:** forthcoming
<br>
<br>

## Tests
> To run API Integration tests, please set the RUN_API_INTEGRATION flag in the API Integration Tests Config section of the `helpers/config` file to TRUE. 

### Local
#### R tests
Run the full test suite from the repo root:
```bash
Rscript tests/testthat_run.r
```

Run a single test file:
```bash
R -e 'testthat::test_file("tests/testthat/test_do_file_parsing.r")'
```
#### Python tests
Run the full Python test suite from the repo root:
```bash
python -m pytest
```

Run a single test file:
```bash
python -m pytest tests/pytest/test_ingestion_pipeline.py
```

### Databricks
#### R tests
Import the repository into a Databricks workspace, then run `tests/testthat_run` as a notebook. The notebook uses `%run` commands to execute each test file in sequence.
> Please note that testthat is not able to crawl the testthat folder in Databricks, therefore if a new testing script is created, it needs to be added to the notebook manually to be run in Databricks.

#### Python tests
Import the repository into a Databricks workspace, then run `tests/pytest_run` as a notebook. Please note that this will work only if the repository is added in a `/Workspace/Repos` folder. If the repository is uploaded in a `/Workspace/Users/` folder, the subprocess won't be able to identify pytest scripts. 


