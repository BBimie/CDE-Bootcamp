import psycopg2
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

class LoadPageViews:
    def __init__(self, companies_pageviews, day, hour, db_config:str):
        """
        data: dict of company -> view count
        db_config: dict with keys host, dbname, user, password, port
        """
        self.companies_pageviews = companies_pageviews
        self.day = day
        self.hour = hour
        self.db_config = db_config

        # Create the hook
        self.hook = PostgresHook(postgres_conn_id=self.db_config)

    def setup_database(self):
        """
        Creates the 'pageviews_hourly' table if it doesn't already exist.
        """
        create_companies_table_sql = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company_name TEXT NOT NULL,
            domain_code TEXT NOT NULL, 
            page_title TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(page_title)
        ); """

        create_pageviews_table_sql = """
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
        try:
            # Use a single connection to run both commands
            with self.hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    logging.info("Creating 'companies' table if not exists...")
                    cursor.execute(create_companies_table_sql)
                    
                    logging.info("Creating 'pageviews_hourly' table if not exists...")
                    cursor.execute(create_pageviews_table_sql)
                conn.commit()
            logging.info("Database schema is ready.")
            
        except Exception as e:
            logging.error(f"Error creating or altering tables: {e}")
            raise

    def load_data(self):
        """
        Reads the dictionary and inserts data into the two tables
        using a single transaction.
        """
        logging.info(f"Starting data load for Oct {self.day}, {self.hour} pageviews")
        
        # SQL to "upsert" company info. If the page_title already
        # exists, it does nothing. It then returns the 'id'.
        upsert_company_sql = """
        INSERT INTO companies (company_name, domain_code, page_title)
        VALUES (%s, %s, %s)
        ON CONFLICT (page_title) DO NOTHING
        RETURNING id;
        """

        # We also need a query in case the INSERT...ON CONFLICT...DO NOTHING
        # doesn't return an ID (because it did nothing).
        select_company_sql = "SELECT id FROM companies WHERE page_title = %s;"
        
        # SQL to "upsert" the hourly data.
        insert_pageview_sql = """
        INSERT INTO pageviews_hourly 
            (company_id, day, hour, view_count, response_size)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (day, hour, company_id) DO NOTHING;
        """
        
        rows_added = 0
        pageview_rows_to_insert = []

        try:
            # Use a single connection and transaction for the entire operation
            with self.hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    
                    # --- Part 1: Upsert companies and get their IDs ---
                    for company_name, data in self.companies_pageviews.items():
                        try:
                            # Run the upsert
                            cursor.execute(upsert_company_sql, (
                                company_name,
                                data['domain_code'],
                                data['page_title']
                            ))
                            
                            result = cursor.fetchone()
                            
                            if result:
                                # inserted a new company
                                company_id = result[0]
                            else:
                                # The company already existed, so the INSERT was
                                # skipped.select its ID.
                                cursor.execute(select_company_sql, (data['page_title'],))
                                company_id = cursor.fetchone()[0]

                            # Prepare the pageview data for bulk insert
                            pageview_tuple = (
                                company_id,
                                self.day,
                                self.hour,
                                int(data['views']),
                                int(data['resp_size'])
                            )
                            pageview_rows_to_insert.append(pageview_tuple)

                        except (KeyError, ValueError, TypeError) as e:
                            logging.warning(f"Skipping data for '{company_name}': {e}")
                    
                    # Bulk insert all pageview data ---
                    if not pageview_rows_to_insert:
                        logging.warning("No valid pageview data to insert.")
                    else:
                        cursor.executemany(insert_pageview_sql, pageview_rows_to_insert)
                        rows_added = cursor.rowcount
                        logging.info(f"Inserted/updated {len(pageview_rows_to_insert)} company records.")
                        
                # Commit the transaction ---
                conn.commit()

            logging.info(f"Data load complete. Added {rows_added} new pageview rows.")
            
        except Exception as e:
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
        extraction_dict=extraction_results,
        day=int(day),
        hour=int(hour),
        postgres_conn_id='wiki_sentiment'
    )
    
    loader.setup_database()
    loader.load_data()
    
    logging.info("Data loaded into PostgreSQL.")
    return "Load complete."