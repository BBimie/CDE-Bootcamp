from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import logging

# # postgress DB connection details from .env
DB_HOST = "postgres"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_NAME = "wikipedia"

def companies_by_top_views(day, hour):

    sql = text(""" SELECT "companies"."company_name", "pageviews_hourly"."view_count" 
               FROM "companies" JOIN "pageviews_hourly" ON "pageviews_hourly"."company_id" = "companies"."id" 
               WHERE "pageviews_hourly"."day" = 22 
                AND "pageviews_hourly"."hour" = 10 
               ORDER BY "pageviews_hourly"."view_count" DESC;
            """)
    
    params= {
        "day_value": day,
        "hour_value": hour
        }
    
    try:
        engine_url = (f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
        if not engine_url:
            raise ValueError("Could not build engine URL from Airflow connection 'my_postgres_db'.")
        
        #connect to default postgres db
        engine = create_engine(engine_url)
    
        with engine.connect() as conn:
            # Pass the params_dict as the second argument to execute()
            result = conn.execute(sql, params)
            
            # Get all rows from the result
            rows = result.fetchall()
            
            if not rows:
                logging.info(f"No data found for day={day}, hour={hour}.")
            else:
                logging.info(f"Query results for day={day}, hour={hour}:")
                for row in rows:
                    # You can access columns by their names
                    logging.info(f"  Company: {row.company_name}, Total Views: {row.view_count}")
                    
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def analysis_callable(**context):
    logging.info("--- Starting Sentiment Analysis Task ---")
    #Get day/hour from params to tag the data
    params = context.get('params', {})
    day = params.get('day')
    hour = params.get('hour')

    if day is None or hour is None:
        raise ValueError("'day' or 'hour' not found in DAG params.")
    
    #run analysis
    companies_by_top_views(day=day, hour=hour)
    logging.info("Sentiment Analysis Query ran successfully.")