from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import Interval, Layer, Owner

from brz_index_daily.constants import INDEX_DATA_S3_KEY, INDEX_TMP_FILE_PATH, SYMBOLS
from brz_index_daily.extractors import fetch_index_data

with DAG(
    dag_id="brz_index_daily",
    default_args={
        "owner": Owner.MINHYEOK,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 * * 1-5",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=[Layer.BRONZE, Interval.DAILY.label],
) as dag:
    fetch_index_data_task = PythonOperator(
        task_id="fetch_index_data",
        python_callable=fetch_index_data,
        provide_context=True,
        op_kwargs={
            "symbols": SYMBOLS,
            "index_tmp_file_path": INDEX_TMP_FILE_PATH,
            "index_data_s3_key": INDEX_DATA_S3_KEY,
        },
    )

    fetch_index_data_task
