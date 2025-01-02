from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import Interval, Layer, Owner

from brz_coin_daily import (
    COIN_DATA_S3_KEY,
    COIN_TMP_FILE_PATH,
    SYMBOLS,
    fetch_coin_data,
)

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
            "coin_data_s3_key": COIN_DATA_S3_KEY,
        },
        provide_context=True,
    )

    fetch_coin_data_task
