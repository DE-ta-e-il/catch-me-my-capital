from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import Interval, Layer, Owner

from brz_news_weekly.constants import NEWS_DATA_S3_KEY, NEWS_TMP_PATH, NYT_API_KEY
from brz_news_weekly.extractors import fetch_news_data

default_args = {
    "owner": Owner.MINHYEOK,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="brz_news_weekly",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2024, 12, 22),
    catchup=True,
    tags=[Layer.BRONZE, Interval.WEEKLY.label],
) as dag:
    fetch_news_data_task = PythonOperator(
        task_id="fetch_news_data",
        python_callable=fetch_news_data,
        provide_context=True,
        op_kwargs={
            "temp_file_path": NEWS_TMP_PATH,
            "api_key": NYT_API_KEY,
            "news_data_s3_key": NEWS_DATA_S3_KEY,
        },
    )

    fetch_news_data_task
