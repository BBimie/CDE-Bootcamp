from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import your local pipeline modules
from scripts.download_pageviews import DownloadPageViews
from scripts.extract_pageviews import ExtractPageViews
from scripts.load_pageviews import LoadPageViews

default_args = {
    "owner": "bimbola",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# def run_download(**context):
#     downloader = DownloadPageViews()
#     downloader.download_file()
#     # Save the extracted file path for next task
#     context["ti"].xcom_push(key="extracted_path", value=str(downloader.extracted_path))

# def run_extract(**context):
#     from datetime import datetime
#     extracted_path = context["ti"].xcom_pull(key="extracted_path")
#     extractor = ExtractPageViews(extracted_path)
#     data = extractor.extract()
#     snapshot_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     context["ti"].xcom_push(key="data", value=data)
#     context["ti"].xcom_push(key="snapshot_datetime", value=snapshot_datetime)

# def run_load(**context):
#     data = context["ti"].xcom_pull(key="data")
#     snapshot = context["ti"].xcom_pull(key="snapshot_datetime")
#     loader = LoadPageViews()
#     loader.load(data, snapshot)

with DAG(
    dag_id="wikipedia_pageviews_dag",
    default_args=default_args,
    description="Wikipedia Pageviews ETL (Download → Extract → Load)",
    start_date=datetime(2025, 10, 20),
    schedule_interval=None,
    catchup=False,
) as dag:
    pass

    # download_task = PythonOperator(
    #     task_id="download_pageviews",
    #     python_callable=run_download,
    #     provide_context=True,
    # )

    # extract_task = PythonOperator(
    #     task_id="extract_pageviews",
    #     python_callable=run_extract,
    #     provide_context=True,
    # )

    # load_task = PythonOperator(
    #     task_id="load_pageviews",
    #     python_callable=run_load,
    #     provide_context=True,
    # )

    # download_task >> extract_task >> load_task
