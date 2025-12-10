# Databricks notebook source
TABLE_QULIFIER =  "prd_csc_mega.sgld48."
metadata_table = "prd_csc_mega.sgld48._ingestion_metadata"
metadata = spark.table(metadata_table)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("countrycode", StringType(), True, metadata={"label": "ISO 3 Letter country code", "notes": ""}),
    StructField("survname", StringType(), True, metadata={"label": "Survey acronym", "notes": "No spaces, no underscores, split sections by \"-\" (e.g. \"ETC-II\")"}),
    StructField("survey", StringType(), True, metadata={"label": "Survey long name", "notes": "Possible names are: LFS, LSMS, …   [I am unsure of this difference, some surveys contain either this or the previous variable, have yet to see one with both]"}),
    StructField("icls_v", StringType(), True, metadata={"label": "Version of the ICLS followed", "notes": "Defines the labor force definitions used according to the rules set out by the nth International Conference of Labour Statisticians."}),
    StructField("isced_version", StringType(), True, metadata={"label": "Version of ISCED used", "notes": ""}),
    StructField("isco_version", StringType(), True, metadata={"label": "Version of ISCO used", "notes": ""}),
    StructField("isic_version", StringType(), True, metadata={"label": "Verstion of ISIC used", "notes": ""}),
    StructField("year", IntegerType(), True, metadata={"label": "Year of survey start", "notes": ""}),
    StructField("vermast", StringType(), True, metadata={"label": "Master (Source) data version", "notes": ""}),
    StructField("veralt", StringType(), True, metadata={"label": "Alternate (Harmonized) data version", "notes": ""}),
    StructField("harmonization", StringType(), True, metadata={"label": "Kind of harmonization", "notes": ""}),
    StructField("int_year", IntegerType(), True, metadata={"label": "Year of interview start", "notes": "For HH and Individual interviews in that HH earliest possible date"}),
    StructField("int_month", IntegerType(), True, metadata={"label": "Month of interview start", "notes": "For HH and Individual interviews in that HH earliest possible date"}),
    StructField("hhid", StringType(), True, metadata={"label": "Household ID", "notes": ""}),
    StructField("pid", StringType(), True, metadata={"label": "Personal ID", "notes": ""}),
    StructField("weight", DoubleType(), True, metadata={"label": "Survey weights", "notes": ""}),
    StructField("weight_m", DoubleType(), True, metadata={"label": "Survey weights for each month", "notes": "If present in the data and the survey reports calculate estimates per month, weight to national level for each month"}),
    StructField("weight_q", DoubleType(), True, metadata={"label": "Survey weights for each quarter", "notes": "If present in the data and the survey reports calculate estimates per quarter, weight to national level for each quarter"}),
    StructField("psu", StringType(), True, metadata={"label": "Primary Sampling unit", "notes": ""}),
    StructField("ssu", StringType(), True, metadata={"label": "Secondary Sampling unit", "notes": ""}),
    StructField("wave", StringType(), True, metadata={"label": "Wave of the survey (e.g., Q1 for quarter 1)", "notes": ""}),
    StructField("panel", StringType(), True, metadata={"label": "Panel the indvidual belongs to", "notes": "Only to be filled out if concept present in raw data, otherwise left missing. No attempt to create panel info during harmonization if not already in data."}),
    StructField("visit_no", IntegerType(), True, metadata={"label": "Number of the visit within the panel", "notes": "Only to be filled out if concept present in raw data, otherwise left missing. No attempt to create visit number info during harmonization if not already in data."}),
    StructField("urban", IntegerType(), True, metadata={"label": "Binary - Individual in urban area", "notes": ""}),
    StructField("subnatid[i]", StringType(), True, metadata={"label": "Subnational ID - [ith] level", "notes": "Subnational ID at the ith level, listing as many as available"}),
    StructField("subnatidsurvey", StringType(), True, metadata={"label": "Lowest level of Subnational ID", "notes": "subnatidsurvey is a string variable that refers to the lowest level of the administrative level at which the survey is representative. In most cases this will be equal to “subnatid1” or “subnatid2”. However, in some cases the lowest level is classified in terms of urban, rural or any other regional categorization cannot be mapped to subnatids. The variable would contain survey representation at lowest level irrespective of its mapping to subnatids."}),
    StructField("subnatid[i]_prev", StringType(), True, metadata={"label": "Subnatid previous - [ith] level", "notes": "Previous subnatid if changed since last survey"}),
    StructField("strata", StringType(), True, metadata={"label": "Strata", "notes": ""}),
    StructField("gaul_adm[i]_code", StringType(), True, metadata={"label": "GAUL ADM[i] code", "notes": "See en.wikipedia.org/wiki/Global_Administrative_Unit_Layers"}),
    StructField("hsize", DoubleType(), True, metadata={"label": "Household size", "notes": ""}),
    StructField("age", DoubleType(), True, metadata={"label": "Age in years", "notes": ""}),
    StructField("male", IntegerType(), True, metadata={"label": "Binary - Individual is male", "notes": ""}),
    StructField("relationharm", IntegerType(), True, metadata={"label": "Relationship to head of household harmonized across all regions", "notes": "GMD - Harmonized categories across all regions. Same as I2D2 categories."}),
    StructField("relationcs", StringType(), True, metadata={"label": "Relationship to head of household country/region specific", "notes": "country or regionally specific categories"}),
    StructField("marital", IntegerType(), True, metadata={"label": "Marital status", "notes": ""}),
    StructField("eye_dsablty", IntegerType(), True, metadata={"label": "Difficulty seeing", "notes": "See \"Recommended Short Set of Questions\" on https://www.cdc.gov/nchs/washington_group/wg_questions.htm"}),
    StructField("hear_dsablty", IntegerType(), True, metadata={"label": "Difficulty hearing", "notes": ""}),
    StructField("walk_dsablty", IntegerType(), True, metadata={"label": "Difficulty walking / steps", "notes": ""}),
    StructField("conc_dsord", IntegerType(), True, metadata={"label": "Difficulty concentrating", "notes": ""}),
    StructField("slfcre_dsablty", IntegerType(), True, metadata={"label": "Difficulty w/ selfcare", "notes": ""}),
    StructField("comm_dsablty", IntegerType(), True, metadata={"label": "Difficulty communicating", "notes": ""}),
    StructField("migrated_mod_age", IntegerType(), True, metadata={"label": "Migration module application age", "notes": ""}),
    StructField("migrated_ref_time", IntegerType(), True, metadata={"label": "Reference time applied to migration questions", "notes": "If migrated_ref_time = 5 means questions about migration refer to any migration in the last 5 years"}),
    StructField("migrated_binary", IntegerType(), True, metadata={"label": "Individual has migrated", "notes": ""}),
    StructField("migrated_years", IntegerType(), True, metadata={"label": "Years since latest migration", "notes": "Years since last migration is the same as how long lived at current location"}),
    StructField("migrated_from", IntegerType(), True, metadata={"label": "Migrated from area", "notes": "No means migrated from rural area"}),
    StructField("migrated_from_cat", IntegerType(), True, metadata={"label": "Category of migration area", "notes": ""}),
    StructField("migrated_from_code", IntegerType(), True, metadata={"label": "Code of migration area", "notes": ""}),
    StructField("migrated_from_country", StringType(), True, metadata={"label": "Code of migration country", "notes": ""}),
    StructField("migrated_reason", IntegerType(), True, metadata={"label": "Reason for migrating", "notes": ""}),
    StructField("ed_mod_age", IntegerType(), True, metadata={"label": "Education module minumum age", "notes": ""}),
    StructField("school", IntegerType(), True, metadata={"label": "Currently in school", "notes": ""}),
    StructField("literacy", IntegerType(), True, metadata={"label": "Individual can read and write", "notes": ""}),
    StructField("educy", IntegerType(), True, metadata={"label": "Years of education", "notes": ""}),
    StructField("educat7", IntegerType(), True, metadata={"label": "Level of education 7 categories", "notes": "No option for \"Other\", as opposed to I2D2, anything not in these categories is to be set to missing"}),
    StructField("educat5", IntegerType(), True, metadata={"label": "Level of education 5 categories", "notes": ""}),
    StructField("educat4", IntegerType(), True, metadata={"label": "Level of education 4 categories", "notes": "Code as 2 (primary) anyone who has some schooling but has not finished secondary."}),
    StructField("educat_orig", StringType(), True, metadata={"label": "Original education code", "notes": "Code if there is a single original education variable (as is in most cases). If there are two or more variables, leave missing, make a note of it."}),
    StructField("educat_isced", DoubleType(), True, metadata={"label": "International Standard Classification of Education (ISCED A)", "notes": "Codes are for example:   2 Lower secondary education    24 Lower secondary general       242 Sufficient partial level completion, without direct access to upper secondary education  Should be coded as 200, 240, and 242 respectively."}),
    StructField("vocational", IntegerType(), True, metadata={"label": "Ever received vocational training", "notes": ""}),
    StructField("vocational_type", IntegerType(), True, metadata={"label": "Type of vocational training", "notes": ""}),
    StructField("vocational_length_l", IntegerType(), True, metadata={"label": "Length of training in months, lower limit", "notes": ""}),
    StructField("vocational_length_u", IntegerType(), True, metadata={"label": "Length of training in months, upper limit", "notes": ""}),
    StructField("vocational_financed", IntegerType(), True, metadata={"label": "How training was financed", "notes": "If funded with different sources, chose main source"}),
    StructField("minlaborage", DoubleType(), True, metadata={"label": "Labor module application age", "notes": ""}),
    StructField("lstatus", IntegerType(), True, metadata={"label": "Labor status (7-day ref period)", "notes": "In some GMD harmonizations (not in their dictionaries) this is given as lstatus_7. In general, the 7 day reference ones are then for example ocusec_7 and ocusec_2. This is then not neat with ocusec_year, ocusec_year_2. Either ocusec_week, ocusec_year or ocusec_7, ocusec_365."}),
    StructField("potential_lf", IntegerType(), True, metadata={"label": "Potential labour force (7-day ref period)", "notes": "A binary indicator taking a value only if the person is not in the labour force (missing if in LF or unemployed). Codes 1 if i) available but not searching or ii) searching but not immediately available to work. Codes 0 otherwise."}),
    StructField("underemployment", IntegerType(), True, metadata={"label": "Underemployment (7-day ref period)", "notes": "A binary indicator taking value only if the person is in the labour force and working (missing if not or unemployed). Codes 1 if person would take on more jobs or more hours at their job if possible/available, 0 otherwise."}),
    StructField("nlfreason", IntegerType(), True, metadata={"label": "Reason not in the labor force  (7-day ref period)", "notes": ""}),
    StructField("unempldur_l", DoubleType(), True, metadata={"label": "Unemployment duration (months)  lower bracket (7-day ref period)", "notes": ""}),
    StructField("unempldur_u", DoubleType(), True, metadata={"label": "Unemployment duration (months) upper bracket (7-day ref period)", "notes": ""}),
    StructField("empstat", IntegerType(), True, metadata={"label": "Employment status, primary job (7-day ref period)", "notes": ""}),
    StructField("ocusec", IntegerType(), True, metadata={"label": "Sector of activity, primary job (7-day ref period)", "notes": "NGOs were classified in I2D2 as public sector, switched to private."}),
    StructField("industry_orig", StringType(), True, metadata={"label": "Original industry code, primary job (7-day ref period)", "notes": ""}),
    StructField("industrycat_isic", StringType(), True, metadata={"label": "ISIC code of the classification, primary job (7-day ref period)", "notes": "Code of ISIC - 4 for example is  Q - Human health and social work activities    86 Human health activities       861          8610 Hospital activities          8620 Medical dental practice activities  The four options would be coded as \"Q\", 8600, 8610, 8620 respectively."}),
    StructField("industrycat10", IntegerType(), True, metadata={"label": "1 digit industry classification, primary job (7-day ref period)", "notes": ""}),
    StructField("industrycat4", IntegerType(), True, metadata={"label": "4-category industry classification, primary job (7-day ref period)", "notes": ""}),
    StructField("occup_orig", StringType(), True, metadata={"label": "Original occupational classification, primary job (7-day ref period)", "notes": ""}),
    StructField("occup_isco", StringType(), True, metadata={"label": "ISCO code of the classification, primary job (7-day ref period)", "notes": "ISCO-08 codes are  8 Plant and machine operators, and assemblers    81 Stationary plant machine operators       811 Mining and mineral processing plant operators          8111 Miners and quarriers  Given the level of detail available, this are to be coded as 8000, 8100, 8110, and 8111 respectively.  Note army occupations are coded 0###."}),
    StructField("occup_skill", IntegerType(), True, metadata={"label": "Skill level based on ISCO standard", "notes": "https://ilostat.ilo.org/resources/concepts-and-definitions/classification-occupation/"}),
    StructField("occup", IntegerType(), True, metadata={"label": "1 digit occupational classification, primary job (7-day ref period)", "notes": ""}),
    StructField("wage_no_compen", DoubleType(), True, metadata={"label": "Last wage payment, primary job, excl. bonuses, etc. (7-day ref period)", "notes": ""}),
    StructField("unitwage", IntegerType(), True, metadata={"label": "Time unit of last wages  payment, primary job (7-day ref period)", "notes": ""}),
    StructField("whours", DoubleType(), True, metadata={"label": "Hours of work in last week, primary job (7-day ref period)", "notes": ""}),
    StructField("wmonths", DoubleType(), True, metadata={"label": "Months worked in the last 12 months, primary job (7-day ref period)", "notes": "This definition may appear confusing since it is months out of the past 12 months of work for the 7 day recall and there is a wmonths_year variable for the 12 month recall. It is not clearly defined in the guidelines, yet I would read it as the number of months in the main job for the 7 day recall job, which would be fewer than the wmonths_year number if the person switched jobs say 2 months ago, had the previous one since over a year."}),
    StructField("wage_total", DoubleType(), True, metadata={"label": "Annualized total wage, primary job (7-day ref period)", "notes": ""}),
    StructField("contract", IntegerType(), True, metadata={"label": "Contract (7-day ref period)", "notes": ""}),
    StructField("healthins", IntegerType(), True, metadata={"label": "Health insurance (7-day ref period)", "notes": ""}),
    StructField("socialsec", IntegerType(), True, metadata={"label": "Social security (7-day ref period)", "notes": ""}),
    StructField("union", IntegerType(), True, metadata={"label": "Union membership (7-day ref period)", "notes": ""}),
    StructField("firmsize_l", DoubleType(), True, metadata={"label": "Firm size (lower bracket), primary job (7-day ref period)", "notes": ""}),
    StructField("firmsize_u", DoubleType(), True, metadata={"label": "Firm size (upper bracket), primary job (7-day ref period)", "notes": ""}),
    StructField("empstat_2", IntegerType(), True, metadata={"label": "Employment status, secondary job (7-day ref period)", "notes": ""}),
    StructField("ocusec_2", IntegerType(), True, metadata={"label": "Sector of activity, secondary job (7-day ref period)", "notes": ""}),
    StructField("industry_orig_2", StringType(), True, metadata={"label": "Original industry code, secondary job (7-day ref period)", "notes": ""}),
    StructField("industrycat_isic_2", IntegerType(), True, metadata={"label": "ISIC code of the classification, secondary job (7-day ref period)", "notes": "See industrycat_isic"}),
    StructField("industrycat10_2", IntegerType(), True, metadata={"label": "1 digit industry classification, secondary job (7-day ref period)", "notes": ""}),
    StructField("industrycat4_2", IntegerType(), True, metadata={"label": "4-category industry classification, secondary job (7-day ref period)", "notes": ""}),
    StructField("occup_orig_2", StringType(), True, metadata={"label": "Original occupational classification, secondary job (7-day ref period)", "notes": ""}),
    StructField("occup_isco_2", StringType(), True, metadata={"label": "ISCO code of the classification, secondary job (7-day ref period)", "notes": "See occup_isco"}),
    StructField("occup_skill_2", IntegerType(), True, metadata={"label": "Skill level based on ISCO standard", "notes": "https://ilostat.ilo.org/resources/concepts-and-definitions/classification-occupation/"}),
    StructField("occup_2", IntegerType(), True, metadata={"label": "1 digit occupational classification, secondary job (7-day ref period)", "notes": ""}),
    StructField("wage_no_compen_2", DoubleType(), True, metadata={"label": "wage payment, secondary job, excl. bonuses, etc. (7-day ref period)", "notes": ""}),
    StructField("unitwage_2", IntegerType(), True, metadata={"label": "Time unit of last wages payment, secondary job (7-day ref period)", "notes": ""}),
    StructField("whours_2", DoubleType(), True, metadata={"label": "Hours of work in last week, secondary job (7-day ref period)", "notes": ""}),
    StructField("wmonths_2", DoubleType(), True, metadata={"label": "Months worked in the last 12 months, secondary job (7-day ref period)", "notes": "See note on wmonths"}),
    StructField("wage_total_2", DoubleType(), True, metadata={"label": "Annualized total wage, secondary job (7-day ref period)", "notes": ""}),
    StructField("firmsize_l_2", DoubleType(), True, metadata={"label": "Firm size (lower bracket), secondary job (7-day ref period)", "notes": ""}),
    StructField("firmsize_u_2", DoubleType(), True, metadata={"label": "Firm size (upper bracket), secondary job (7-day ref period)", "notes": ""}),
    StructField("t_hours_others", DoubleType(), True, metadata={"label": "Total hours of work in the last 12 months in other jobs excluding the primary and secondary ones", "notes": "There is a similar potential for confusion here than with wmonths as the definition is for 12 months and the recall is for 7 days."}),
    StructField("t_wage_nocompen_others", DoubleType(), True, metadata={"label": "Annualized wage in all jobs excluding the primary and secondary ones (excluding tips, bonuses, etc.).", "notes": ""}),
    StructField("t_wage_others", DoubleType(), True, metadata={"label": "Annualized wage (including tips, bonuses, etc.) in all other jobs excluding the primary and secondary ones. (7 day ref).", "notes": ""}),
    StructField("t_hours_total", DoubleType(), True, metadata={"label": "Annualized hours worked in all jobs (7-day ref period)", "notes": ""}),
    StructField("t_wage_nocompen_total", DoubleType(), True, metadata={"label": "Annualized wage in all jobs excl. bonuses, etc. (7-day ref period)", "notes": ""}),
    StructField("t_wage_total", DoubleType(), True, metadata={"label": "Annualized total wage for all jobs (7-day ref period)", "notes": ""}),
    StructField("lstatus_year", IntegerType(), True, metadata={"label": "Labor status (12-mon ref period)", "notes": "Entering the subsection for the 12 month recall. The GMD guidelines state that:  This section must be filled only for those individuals who responded to labor questions with a reference period of 12 months, regardless of whether they responded to questions with a reference period of the last 7 days."}),
    StructField("potential_lf_year", IntegerType(), True, metadata={"label": "Potential labour force (12-mon ref period)", "notes": "A binary indicator taking a value only if the person is not in the labour force (missing if in LF or unemployed). Codes 1 if i) available but not searching or ii) searching but not immediately available to work. Codes 0 otherwise."}),
    StructField("underemployment_year", IntegerType(), True, metadata={"label": "Underemployment (12-mon ref period)", "notes": "A binary indicator taking value only if the person is in the labour force and working (missing if not or unemployed). Codes 1 if person would take on more jobs or more hours at their job if possible/available, 0 otherwise."}),
    StructField("nlfreason_year", IntegerType(), True, metadata={"label": "Reason not in the labor force (12-mon ref period)", "notes": ""}),
    StructField("unempldur_l_year", DoubleType(), True, metadata={"label": "Unemployment duration (months) lower bracket (12-mon ref period)", "notes": ""}),
    StructField("unempldur_u_year", DoubleType(), True, metadata={"label": "Unemployment duration (months) upper bracket (12-mon ref period)", "notes": ""}),
    StructField("empstat_year", IntegerType(), True, metadata={"label": "Employment status, primary job (12-mon ref period)", "notes": ""}),
    StructField("ocusec_year", IntegerType(), True, metadata={"label": "Sector of activity, primary job (12-mon ref period)", "notes": ""}),
    StructField("industry_orig_year", StringType(), True, metadata={"label": "Original industry code, primary job (12-mon ref period)", "notes": ""}),
    StructField("industrycat_isic_year", IntegerType(), True, metadata={"label": "ISIC code of the classification, primary job (12 month ref period)", "notes": "See industrycat_isic"}),
    StructField("industrycat10_year", IntegerType(), True, metadata={"label": "1 digit industry classification, primary job (12-mon ref period)", "notes": ""}),
    StructField("industrycat4_year", IntegerType(), True, metadata={"label": "4-category industry classification primary job (12-mon ref period)", "notes": ""}),
    StructField("occup_orig_year", StringType(), True, metadata={"label": "Original occupational classification, primary job (12-mon ref period)", "notes": ""}),
    StructField("occup_isco_year", StringType(), True, metadata={"label": "ISCO code of the classification, primary job (12 month ref period)", "notes": "See occup_isco"}),
    StructField("occup_skill_year", IntegerType(), True, metadata={"label": "Skill level based on ISCO standard", "notes": "https://ilostat.ilo.org/resources/concepts-and-definitions/classification-occupation/"}),
    StructField("occup_year", IntegerType(), True, metadata={"label": "1 digit occupational classification, primary job (12-mon ref period)", "notes": ""}),
    StructField("wage_no_compen_year", DoubleType(), True, metadata={"label": "Last wage payment, primary job, excl. bonuses, etc. (12-mon ref period)", "notes": ""}),
    StructField("unitwage_year", IntegerType(), True, metadata={"label": "Time unit of last wages payment, primary job (12-mon ref period)", "notes": ""}),
    StructField("whours_year", DoubleType(), True, metadata={"label": "Hours of work in last week, primary job (12-mon ref period)", "notes": ""}),
    StructField("wmonths_year", DoubleType(), True, metadata={"label": "Months worked in the last 12 months, primary job (12-mon ref period)", "notes": ""}),
    StructField("wage_total_year", DoubleType(), True, metadata={"label": "Annualized total wage, primary job (12-mon ref period)", "notes": ""}),
    StructField("contract_year", IntegerType(), True, metadata={"label": "Contract (12-mon ref period)", "notes": ""}),
    StructField("healthins_year", IntegerType(), True, metadata={"label": "Health insurance (12-mon ref period)", "notes": ""}),
    StructField("socialsec_year", IntegerType(), True, metadata={"label": "Social security (12-mon ref period)", "notes": ""}),
    StructField("union_year", IntegerType(), True, metadata={"label": "Union membership (12-mon ref period)", "notes": ""}),
    StructField("firmsize_l_year", DoubleType(), True, metadata={"label": "Firm size (lower bracket), primary job (12-mon ref period)", "notes": ""}),
    StructField("firmsize_u_year", DoubleType(), True, metadata={"label": "Firm size (upper bracket), primary job (12-mon ref period)", "notes": ""}),
    StructField("empstat_2_year", IntegerType(), True, metadata={"label": "Employment status, secondary job (12-mon ref period)", "notes": ""}),
    StructField("ocusec_2_year", IntegerType(), True, metadata={"label": "Sector of activity, secondary job (12-mon ref period)", "notes": ""}),
    StructField("industry_orig_2_year", StringType(), True, metadata={"label": "Original industry code, secondary job (12-mon ref period)", "notes": ""}),
    StructField("industrycat_isic_2_year", IntegerType(), True, metadata={"label": "ISIC code of the classification, secondary job (12 month ref period)", "notes": "See industrycat_isic"}),
    StructField("industrycat10_2_year", IntegerType(), True, metadata={"label": "1 digit industry classification, secondary job (12-mon ref period)", "notes": ""}),
    StructField("industrycat4_2_year", IntegerType(), True, metadata={"label": "4-category industry classification, secondary job (12-mon ref period)", "notes": ""}),
    StructField("occup_orig_2_year", StringType(), True, metadata={"label": "Original occupational classification, secondary job (12-mon ref period)", "notes": ""}),
    StructField("occup_isco_2_year", StringType(), True, metadata={"label": "ISCO code of the classification, secondary job (12 month ref period)", "notes": "See occup_isco"}),
    StructField("occup_skill_2_year", IntegerType(), True, metadata={"label": "Skill level based on ISCO standard", "notes": "https://ilostat.ilo.org/resources/concepts-and-definitions/classification-occupation/"}),
    StructField("occup_2_year", IntegerType(), True, metadata={"label": "1 digit occupational classification, secondary job (12-mon ref period)", "notes": ""}),
    StructField("wage_no_compen_2_year", DoubleType(), True, metadata={"label": "wage payment, secondary job, excl. bonuses, etc. (12-mon ref period)", "notes": ""}),
    StructField("unitwage_2_year", IntegerType(), True, metadata={"label": "Time unit of last wages payment, secondary job (12-mon ref period)", "notes": ""}),
    StructField("whours_2_year", DoubleType(), True, metadata={"label": "Hours of work in last week, secondary job (12-mon ref period)", "notes": ""}),
    StructField("wmonths_2_year", DoubleType(), True, metadata={"label": "Months worked in the last 12 months, secondary job (12-mon ref period)", "notes": "See note on wmonths"}),
    StructField("wage_total_2_year", DoubleType(), True, metadata={"label": "Annualized total wage, secondary job (12-mon ref period)", "notes": ""}),
    StructField("firmsize_l_2_year", DoubleType(), True, metadata={"label": "Firm size (lower bracket), secondary job (12-mon ref period)", "notes": ""}),
    StructField("firmsize_u_2_year", DoubleType(), True, metadata={"label": "Firm size (upper bracket), secondary job (12-mon ref period)", "notes": ""}),
    StructField("t_hours_others_year", DoubleType(), True, metadata={"label": "Annualized hours worked in all but primary & secondary jobs (12-mon ref period)", "notes": ""}),
    StructField("t_wage_nocompen_others_year", DoubleType(), True, metadata={"label": "Annualized wage in all but primary & secondary jobs excl. bonuses, etc. (12-mon ref period)", "notes": ""}),
    StructField("t_wage_others_year", DoubleType(), True, metadata={"label": "Annualized wage in all but primary and secondary jobs (12-mon ref period)", "notes": ""}),
    StructField("t_hours_total_year", DoubleType(), True, metadata={"label": "Annualized hours worked in all jobs (12-mon ref period)", "notes": ""}),
    StructField("t_wage_nocompen_total_year", DoubleType(), True, metadata={"label": "Annualized wage in all jobs excl. bonuses, etc. (12-mon ref period)", "notes": ""}),
    StructField("t_wage_total_year", DoubleType(), True, metadata={"label": "Annualized total wage for all jobs (12-mon ref period)", "notes": ""}),
    StructField("njobs", DoubleType(), True, metadata={"label": "Total number of jobs", "notes": "This and the following three variable in GMD codify the so called \"Total Labor Income\". The note in the guidelines states:  Total Labor income will be created based on either the 7 days or 12 months reference period variables or a combination of both. Harmonizers should make sure that all jobs are included and none of them are double counted. "}),
    StructField("t_hours_annual", DoubleType(), True, metadata={"label": "Total hours worked in all jobs in the previous 12 months", "notes": ""}),
    StructField("linc_nc", DoubleType(), True, metadata={"label": "Total annual wage income in all jobs, excl. bonuses, etc.", "notes": "Difference to t_wage_nocompen_total_year?"}),
    StructField("laborincome", DoubleType(), True, metadata={"label": "Total annual wage income in all jobs", "notes": "Difference to t_wage_total_year? Same note applies as for njobs and linc_nc."})
])

# COMMAND ----------

#TODO add this to the metadata table as a flag
TO_REMOVE = ['MEX_2005-2023_ENOE_V01_M_V01_A_GLD', 'IND_2022_PLFS-Urban-Panel_V01_M_V01_A_GLD']

# COMMAND ----------

from pyspark.sql.functions import col, lit

# Get distinct table names from the ingestion metadata
# Get distinct table names from the ingestion metadata where ingested is True
table_names = (
    metadata.filter(col("ingested") == True)
            .select("table_name")
            .distinct()
            .rdd.flatMap(lambda r: r)
            .collect()
)
table_names.sort()
# List of expected columns from the predefined schema
expected_cols = [f.name for f in schema.fields]

# Build a stacked DataFrame by iterating over each source table
stacked_df = None
for tbl in table_names:
    if tbl in TO_REMOVE:
        continue

    # ------------------------------------------------------------------
    # Extract country, year, and survey identifiers from the table name
    # Expected format: COUNTRY_YEAR_SURVEY_... (e.g. TZA_2014_ILFS-TAN_...)
    # ------------------------------------------------------------------
    print(tbl)
    parts = tbl.split("_")
    country_val = parts[0] if len(parts) > 0 else None
    year_val = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    survey_val = parts[2] if len(parts) > 2 else None
    tbl = TABLE_QULIFIER + tbl
    src_df = spark.table(tbl)

    # Identify extra columns not present in the schema
    extra_cols = [c for c in src_df.columns if c not in expected_cols]
    extra_flag = lit(bool(extra_cols)).alias("extra_columns_flag")

    # Select expected columns, filling missing ones with nulls or extracted values
    selected_exprs = []
    for c in expected_cols:
        if c == "countrycode" and country_val is not None:
            selected_exprs.append(lit(country_val).cast(schema[c].dataType).alias(c))
        elif c == "year" and year_val is not None:
            selected_exprs.append(lit(year_val).cast(schema[c].dataType).alias(c))
        elif c == "survname" and survey_val is not None:
            selected_exprs.append(lit(survey_val).cast(schema[c].dataType).alias(c))
        else:
            selected_exprs.append(col(c) if c in src_df.columns else lit(None).cast(schema[c].dataType).alias(c))

    # Append the extra‑columns flag
    selected_exprs.append(extra_flag)

    # Create a DataFrame that conforms to the schema (plus flag)
    aligned_df = src_df.select(*selected_exprs)

    # Union all tables together
    aligned_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TABLE_QULIFIER + "GLD_HARMONIZED")

# COMMAND ----------



# COMMAND ----------


