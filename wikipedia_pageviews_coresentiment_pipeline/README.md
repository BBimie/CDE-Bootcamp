# Wikipedia Pageviews ETL Pipeline with Airflow

This project uses Apache Airflow to create an ETL pipeline that downloads the Wikipedia pageview data for October 22, 10am for specific companies (Apple, Amazon, Google, Facebook, Microsoft) and loads this data into the airflow PostgreSQL database.

It also allows the user to pass in the day and hour they want using airflow's XCom.


### Project Directory Overview

```wikipedia_pageviews_coresentiment_pipeline/
    ├── docker-compose.yaml
    ├── dags/
    │    ├── pageviews_sentiment_dag.py
    │    └── scripts/
    │           ├── download_pageviews.py
    │           ├── extract_pageviews.py
    │           ├── load_pageviews.py
    │           └── sentiment_analysis.py
    │
    ├── requirements.txt
    ├── .gitignore
    └── README.md
```


### Database Schema 
The `wikipedia` database

- `companies` table: Stores information about the tracked companies.

```sql 
CREATE TABLE IF NOT EXISTS companies (
    `id` (SERIAL PRIMARY KEY)
    `company_name` (TEXT)
    `domain_code` (TEXT)
    `page_title` (TEXT UNIQUE)
    `created_at` (TIMESTAMPTZ)
    );
```
- `pageviews_hourly` table: Stores the hourly pageview counts.
```sql
CREATE TABLE IF NOT EXISTS pageviews_hourly (
    `id` (SERIAL PRIMARY KEY)
    `company_id` (INTEGER REFERENCES companies(id))
    `day` (INTEGER)
    `hour` (INTEGER)
    `view_count` (INTEGER)
    `response_size` (BIGINT)
    `created_at` (TIMESTAMPTZ)
    UNIQUE constraint on `(day, hour, company_id)
);
```

## Setup Instructions

1.  **Clone the Repository:**
2.  **Build and Start Services:**
    docker-compose up -d
3.  **Create the Target Database:**
    The `docker-compose.yml` file automatically creates the `airflow` database for Airflow's metadata. You need to manually create the `wikipedia` database where the pageview data will be stored:
    ```bash
    docker-compose exec postgres psql -U airflow -c "CREATE DATABASE wikipedia;"
    ```
4.  **Access Airflow UI:**
    Open your web browser and navigate to `http://localhost:8080`. Log in with the default credentials (username: `airflow`, password: `airflow`).

## Running the DAG

1.  In the Airflow UI, find the DAG named `wikipedia_pageviews_dag`.
2.  Unpause the DAG by clicking the toggle switch on the left to "On".
3.  Trigger the DAG manually by clicking the "Play" button on the right.
4.  When prompted for configuration, provide the day and hour you want to process
5.  Click "Trigger". The DAG will run the download, extract, and load tasks.


### Sample Result
Date Analyzed: October 22, 2025, 10:00 AM

| company_name | view_count | 
| -----------  | ---------- | 
| Google       |        383 | 
| Facebook     |        362 | 
| Amazon       |        162 | 
| Microsoft    |        141 | 
| Apple        |        136 | 
