from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from pathlib import Path
import sys
import logging

from scripts.download_pageviews import download_task_callable
from scripts.extract_pageviews import extract_task_callable
from scripts.load_pageviews import load_task_callable

# Set up logging
logging.basicConfig(level=logging.INFO)

companies = {
            'Amazon': 'en Amazon_(company)',
            'Apple': 'en Apple_Inc.',
            'Facebook': 'en Facebook',
            'Google': 'en Google',
            'Microsoft': 'en Microsoft'
        }


with DAG(
    dag_id='wikipedia_pageviews_dag',
    schedule=None,  # Run manually
    start_date=datetime(2025, 10, 1),
    params={
        "day": 22,
        "hour": 10,
        "companies" : companies
    }
):
    
    download_pageviews_task = PythonOperator(
        task_id="download",
        python_callable=download_task_callable
    )

    extract_pageviews_task = PythonOperator(
        task_id='extract',
        python_callable=extract_task_callable)
    
    load_pageviews_task = PythonOperator(
        task_id='load',
        python_callable=load_task_callable)
    
    download_pageviews_task >> extract_pageviews_task >> load_pageviews_task