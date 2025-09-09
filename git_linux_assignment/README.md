# GIT-LINUX-ASSIGNMENT

This assignment had 3 tasks, which have been separated into different folders.
- ETL using bash scripting
- Move CSV & JSON files using bash scripting
- Load Posey data using bash scripting and analyse using SQL


## ETL Task 1

A simple ETL (Extract, Transform, Load) process using bash scripting.

#### Steps
1. **Extract:** Using the `extract.sh` script
    - Downloaded the data [from](https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv) and stored the the `./raw` folder. 

2. **Transform:**
    - Column rename, `Variable_code` to `variable_code`.
    - Selected only `Year`, `Value`, `Units`, and `variable_code` columns and saved the new transformed data in the `./Transformed` folder.

3. **Load:** 
    - The script then moves the tranformed file in the Transformed folder to a new folder `./Gold`.

4. **Scheduling**
    - This script has been set up to run at 12am everyday on the crontab on my local machine.
    - The cron expression is `0 0 * * *`.

#### Notes
- File url, folder names and the transformed file name are in the `.env` file.
- The script automatically creates all the folders needed; `raw, Transformed & Gold`


## Task 2
## Move CSV & JSON files using bash

#### Directory Management
    - The directories `SOURCE_FOLDER` : the folder to search for .csv and .json files & `DESTINATION_FOLDER` : the folder where files will be moved are defined in the `.env` file.
    - When no `SOURCE_FOLDER` is provided, the script searches its own current directory (.).

#### Finding the files
    - The script uses the file extensions `.csv` & `.json`, and the search is case insensitive (just incase)
    - If no files exist, the script ends
    - If files exist, they are all moved into the `DESTINATION_FOLDER`


## Task 2
## Parch & Posey Analysis

#### ETL
The ETL process is handled by a Bash script `posey_task/Scripts/bash/posey_etl.sh`
    - The script loads the data in the csv files downloaded from [here](https://github.com/jdbarillas/parchposey/tree/master/data-raw)
    - The connection credentials of the postgres DB are defined in the `.env` file 
        `PG_DB_NAME`
        `PG_HOST`
        `PG_PASSWORD`
        `PG_PORT`
        `PG_USER`
    - The script connects to the postgres instance and creates the `PG_DB_NAME` if it does not exist
    - Uses the filename (without the .csv), to create the table names
    - Loads the data in each file into the DB

#### Analysis
4 Questions were provided to be answered using SQL queries, the queries are stored in the `posey_task/Scripts/sql/posey_analysis.sql` folder.

- Find a list of order IDs where either gloss_qty or poster_qty is greater than 4000. Only include the id field in the resulting table.
- Write a query that returns a list of orders where the standard_qty is zero and either the gloss_qty or poster_qty is over 1000.
- Find all the company names that start with a 'C' or 'W', and where the primary contact contains 'ana' or 'Ana', but does not contain 'eana'.
- Provide a table that shows the region for each sales rep along with their associated accounts. Your final table should include three columns: the region name, the sales rep name, and the account name. Sort the accounts alphabetically (A-Z) by account name.