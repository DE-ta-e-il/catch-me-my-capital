from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import Interval, Layer, Owner
from common.uploaders import upload_file_to_s3

from brz_exchange_rate_daily.exchange_rate_constants import (
    CURRENCY_PAIRS,
    EXCHANGE_RATE_DATA_S3_KEY,
    EXCHANGE_RATE_TMP_FILE_PATH,
)
from brz_exchange_rate_daily.exchange_rate_extractors import fetch_exchange_rates

with DAG(
    dag_id="brz_exchange_rate_daily",
    default_args={
        "owner": Owner.MINHYEOK,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 * * 1-5",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=[Layer.BRONZE, Interval.DAILY],
) as dag:
    fetch_exchange_rates_task = PythonOperator(
        task_id="fetch_exchange_rates",
        python_callable=fetch_exchange_rates,
        op_kwargs={
            "currency_pairs": CURRENCY_PAIRS,
            "exchange_rate_tmp_file_path": EXCHANGE_RATE_TMP_FILE_PATH,
        },
        provide_context=True,
    )

    upload_exchange_rates_to_s3_task = PythonOperator(
        task_id="upload_exchange_rates_to_s3",
        python_callable=upload_file_to_s3,
        provide_context=True,
        op_kwargs={
            "file_path": EXCHANGE_RATE_TMP_FILE_PATH,
            "key": EXCHANGE_RATE_DATA_S3_KEY,
        },
    )

    fetch_exchange_rates_task >> upload_exchange_rates_to_s3_task
