# mega-gld
#### Automation pipeline to onboard the Global Labor Database to the Corporate Data Lake and publish its datasets to the Microdata Library
#### 
The pipeline is organized in five steps and split in two branches after the ingestion step.
* Branch a deals with individual tables and their publication
* Branch b deals with stacking the tables and with the publication of a comprehensive table.

This is a list of all support folders/files/tables:
* The **`_ingestion_metadata table`** is a table that keeps track of which tables have been ingested and published in the Microdata Library, as well as of some dataset-specific metadata. It also stores the path to the dta files and do files that have been associated with that specific ingestion
* [This Sharepoint file](https://worldbankgroup.sharepoint.com/:x:/r/sites/dap/gld/_layouts/15/Doc.aspx?sourcedoc=%7B3F8DB34F-8359-4BFC-86A0-5D14776E59E7%7D&file=survey-metadata.xlsx&action=default&mobileredirect=true) keeps track of all country survey specific metadata. 
> When a new survey is onboarded to the GLD catalog, the survey should be added in this csv file. 
* Country codes and names are matched using the MEGA table at prd_mega.indicator.country. 
* The folder /Volumes/prd_csc_mega/sgld48/vgld48/Workspace/**json_to_publish**/ contains all json files awaiting publications. 
> These files are created in Script 3a and deleted once publication is successfully completed in Script 4a. 


## Steps
___
**Script 0 (0-gld-identify-delta.R):** identifies new tables that need to be ingested. To do so, it crawls the entire Sharepoint library to identify "Data/Harmonized" folders and then filters for the highest Master Data Version (M) and Table Version (A).The new identified tables are added to the __ingestion_metadata_ table. If an older version is identified, the table for that older version is removed (this behavior might change).
This script features the creation of the following __ingestion_metadata_ variables: `filename`, `dta_path`, `country`, `year`, `quarter`, `survey`, `M_version`, `A_version`. These are  metadata fields that can be inferred from the filename.
___
**Script 1 (1-gld-ingest-full.py):** ingests all new tables to prd_csc_mega.sgld48, either in their entirety or in 5000-line chunks, if the file exceeds 900MB. The script also saves all variable labels as column comments in Databricks. At the end of the ingestion process, the following variables are updated in __ingestion_metadata_ - `harmonization` (GLD or GLD Light) and `household_level` (TRUE if the dataset contains the hhid variable), and `ingested` (TRUE if the ingestion process is successful).
> Please note that Databricks tables cannot include the character "-" in their name. Therefore, "-" is replaced with "_" and the value correspondence is stored in the `table_name` column of __ingestion_metadata_
___
**Script 2a (2a-gld-metadata-parse.R):** identifies the .do file that corresponds to the .dta file ingested, and parses it to extract the description of changes from the previous version to the current one. 
Both `do_path` and `version_label` are stored in the __ingestion_metadata_ table.
> Once we have agreed on a standardized template for extracting data classification from the "Where is this data from?.txt" files, this script will also contained parsing code for `classification`, and will add `classification` to the __ingestion_metadata_ table.
___
**Script 3a (3a-gld-json-creation.R):** creates all the .json files from metadata in the __ingestion_metadata_ table, using the supporting files _countries.csv_, and _survey_metadata.csv_, and saves them in the json_temp folder. It also computes and verifies github links for those surveys that have documentation published in github.
___
**Script 4a (4a-gls-json-pub.R):** publishes all the json files in json_temp, alongside the corresponding datasets (using the file in the `dta_path` column of the __ingestion_metadata_ table), .do files (using the file in the `do_path` column of the __ingestion_metadata_ table) and technical documentation/questionnaires (found in the corresponding Docs folder). 
> Please note that the environment variable "NADA_API_KEY" is used to publish. if the API key needs to be updated, follow [these instructions](https://docs.databricks.com/aws/en/security/secrets/). The scope is GLDKEYVAULT, and it can be managed by anyone in the ITSDA-LKHS-DAP-PROD-gld team.

> Please note that the structure of the Docs folders varies. Some Docs folders contain files, some contain subfolders called Technical and Questionnaires - however these can be empty. For convenience, if documents are correctly organized, they will be published as two separate resources (Questionnaires and Technical Documentation). If they are not, all documents get uploaded as Technical Documentation. 
___
**Script 2b** [forthcoming. manages stacking]
___
**Script 3b** [forthcoming. manages json creation of the stacked table]
___
**Script 4b** [forthcoming. manages json publication of the stacked table]

## Tests
Run the full test suite from the repo root:
```r
Rscript tests/testthat.R
```

Run a single test file:
```r
R -e 'testthat::test_file("tests/testthat/test_do_file_parsing.R")'
```
