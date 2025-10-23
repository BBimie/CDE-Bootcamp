import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

# Get Airflow-compatible logger
log = logging.getLogger(__name__)

class QueryPageViews:
    """
    Handles querying the PostgreSQL database to rank companies by views
    for a specific day and hour.
    """

    def __init__(self, day: int, hour: int, postgres_conn_id: str):
        self.day = day
        self.hour = hour
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def query_and_rank(self):
        sql = """
            SELECT
                c.company_name,
                ph.view_count
            FROM pageviews_hourly ph
            JOIN companies c ON ph.company_id = c.id
            WHERE ph.day = %(day)s 
                AND ph.hour = %(hour)s
            ORDER BY ph.view_count DESC;
        """
    
        params = {'day': self.day, 'hour': self.hour}
        
        try:
            results = self.hook.get_records(sql=sql, parameters=params)
            
            if not results:
                log.warning("No data found for this day and hour.")
                return None
                
            return results
            
        except Exception as e:
            raise

def query_task_callable(**context):
    logging.info("--- Starting Query Task (PostgreSQL) ---")
    
    params = context.get('params', {})
    day = params.get('day')
    hour = params.get('hour')
    
    if day is None or hour is None:
        raise ValueError("'day' or 'hour' not found in DAG params.")

    # Instantiate the querier
    querier = QueryPageViews(
        day=int(day),
        hour=int(hour),
        postgres_conn_id='postgres_wiki' # Must match your Airflow Connection
    )
    
    # 2. Run the query
    ranked_results = querier.query_and_rank()
    
    # 3. Log the results
    if ranked_results:
        log.info("--- Pageview Rankings ---")
        for i, (company, views) in enumerate(ranked_results):
            log.info(f"#{i+1}: {company} - {views} views")
    else:
        log.info("No results to rank.")
        
    return "Query and logging complete."