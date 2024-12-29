from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.uploaders import upload_file_to_s3

from brz_coin_daily.coin_constants import COIN_DATA_S3_KEY, COIN_TMP_FILE_PATH, SYMBOLS
from brz_coin_daily.coin_extractors import fetch_coin_data
from dags.common.constants import Interval, Layer, Owner

with DAG(
    dag_id="brz_coin_daily",
    default_args={
        "owner": Owner.MINHYEOK,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=[Layer.BRONZE, Interval.DAILY],
) as dag:
    fetch_coin_data_task = PythonOperator(
        task_id="fetch_coin_data",
        python_callable=fetch_coin_data,
        op_kwargs={
            "symbols": SYMBOLS,
            "coin_tmp_file_path": COIN_TMP_FILE_PATH,
        },
        provide_context=True,
    )

    upload_coin_data_to_s3_task = PythonOperator(
        task_id="upload_coin_data_to_s3",
        python_callable=upload_file_to_s3,
        provide_context=True,
        op_kwargs={
            "file_path": COIN_TMP_FILE_PATH,
            "key": COIN_DATA_S3_KEY,
        },
    )

    fetch_coin_data_task >> upload_coin_data_to_s3_task
