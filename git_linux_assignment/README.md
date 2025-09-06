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

4. **Scheduler:**
    - The `scheduler.sh` script automates the execution of `extract.sh` 12am everyday.


#### Notes
- File url, folder names and the transformed file name are in the `.env` file.
- The script automatically creates all the folders needed; `raw, Transformed & Gold`
- 