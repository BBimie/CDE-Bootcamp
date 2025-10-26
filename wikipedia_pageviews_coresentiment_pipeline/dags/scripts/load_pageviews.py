import logging
import os
from sqlalchemy import create_engine, text

class LoadPageViews:
    def __init__(self, companies_pageviews, day, hour,):
        self.companies_pageviews = companies_pageviews
        self.day = day
        self.hour = hour

        # # postgress DB connection details from .env
        self.DB_HOST = "postgres"
        self.DB_USER = "airflow"
        self.DB_PASSWORD = "airflow"
        self.DB_NAME = "wikipedia"

    def setup_database(self):
        #create_database_sql = f""" CREATE DATABASE {self.DB_NAME} ; """

        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company_name TEXT NOT NULL,
            domain_code TEXT NOT NULL, 
            page_title TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(page_title)
        );

        CREATE TABLE IF NOT EXISTS pageviews_hourly (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            day INTEGER NOT NULL,
            hour INTEGER NOT NULL,
            view_count INTEGER NOT NULL,
            response_size BIGINT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(day, hour, company_id)
        );
        """
        engine_url = (f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}")
        if not engine_url:
            raise ValueError("Could not build engine URL from Airflow connection 'my_postgres_db'.")
        
        #connect to default postgres db
        self.engine = create_engine(engine_url)

        #connect to the created db to set up the table
        #self.engine = create_engine(f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}")
        with self.engine.begin() as conn:
            conn.execute(text(create_tables_sql))
            print("Table created successfully.")

    def load_data(self):
        logging.info(f"Starting data load for Oct {self.day}, {self.hour} pageviews")

        upsert_company_sql = text("""
        INSERT INTO companies (company_name, domain_code, page_title)
        VALUES (:company_name, :domain_code, :page_title)
        ON CONFLICT (page_title) DO NOTHING
        RETURNING id;
        """)

        # Query to select the ID if the INSERT...ON CONFLICT did nothing
        select_company_sql = text("SELECT id FROM companies WHERE page_title = :page_title;")
        
        # SQL to "upsert" the hourly data
        insert_pageview_sql = text("""
        INSERT INTO pageviews_hourly 
            (company_id, day, hour, view_count, response_size)
        VALUES (:company_id, :day, :hour, :view_count, :response_size)
        ON CONFLICT (day, hour, company_id) DO NOTHING;
        """)
        
        rows_added = 0
        pageview_params_list = []

        try:
            with self.engine.begin() as conn:
                
                # Upsert companies and get their IDs
                for company_name, data in self.companies_pageviews.items():
                    try:
                        page_title = data['page_title'] # Get once
                        
                        company_params = {
                            'company_name': company_name,
                            'domain_code': data['domain_code'],
                            'page_title': page_title
                        }
                        
                        # Run the upsert and fetch the scalar result (the 'id' or None)
                        result = conn.execute(upsert_company_sql, company_params).scalar()
                        
                        if result:
                            # inserted a new company
                            company_id = result
                        else:
                            # The company already existed, so the INSERT was
                            # skipped. Select its ID.
                            company_id = conn.execute(
                                select_company_sql, 
                                {'page_title': page_title}
                            ).scalar()

                        # This check is good practice, though it shouldn't fail
                        # if the logic is sound.
                        if company_id is None:
                            logging.warning(f"Could not find or create a company ID for {page_title}. Skipping.")
                            continue

                        # Prepare the pageview data as a dict for bulk insert
                        pageview_params = {
                            'company_id': company_id,
                            'day': self.day,
                            'hour': self.hour,
                            'view_count': int(data['views']),
                            'response_size': int(data['resp_size'])
                        }
                        pageview_params_list.append(pageview_params)

                    except (KeyError, ValueError, TypeError) as e:
                        logging.warning(f"Skipping data for '{company_name}': {e}")
                
                # Bulk insert all pageview data ---
                if not pageview_params_list:
                    logging.warning("No valid pageview data to insert.")
                else:
                    # conn.execute with a list of dicts performs an "executemany"
                    result_proxy = conn.execute(insert_pageview_sql, pageview_params_list)
                    rows_added = result_proxy.rowcount
                    logging.info(f"Processed {len(pageview_params_list)} company records.")
                    
                # No explicit .commit() needed; the 'with engine.begin()' handles it.
                logging.info(f"Data load complete. Added {rows_added} new pageview rows.")
                
            
        except Exception as e:
            # No explicit .rollback() needed; 'with engine.begin()' handles it.
            logging.error(f"Database error during data load: {e}")
            raise

def load_task_callable(**context):
    """
    This is the function your PythonOperator will run.
    (This function does not need to change.)
    """
    logging.info("--- Starting Load Task (Normalized PostgreSQL) ---")

    # Pull the pageviews dictionary from the 'extract' task
    task_instance = context['task_instance']
    extraction_results = task_instance.xcom_pull(task_ids='extract')
    
    if not extraction_results or not isinstance(extraction_results, dict):
        raise ValueError("No results dictionary received from XCom. Upstream 'extract' may have failed.")

    logging.info(f"Received extraction dictionary for {list(extraction_results.keys())}")
    
    #Get day/hour from params to tag the data
    params = context.get('params', {})
    day = params.get('day')
    hour = params.get('hour')
    
    if day is None or hour is None:
        raise ValueError("'day' or 'hour' not found in DAG params.")

    # 3. Instantiate and run the load
    loader = LoadPageViews(
        companies_pageviews=extraction_results,
        day=int(day),
        hour=int(hour),
    )
    
    loader.setup_database()
    loader.load_data()
    
    logging.info("Data loaded into PostgreSQL.")
    return "Load complete."