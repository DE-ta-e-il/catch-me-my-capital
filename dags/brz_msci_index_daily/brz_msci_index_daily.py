from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common import Interval, Layer, Owner

from brz_msci_index_daily import (
    MSCI_INDEX_DATA_S3_KEY,
    MSCI_INDEX_TMP_FILE_PATH,
    MSCI_URL_INFO,
    fetch_msci_indices_data,
)

with DAG(
    dag_id="brz_msci_index_daily",
    default_args={
        "owner": Owner.MINHYEOK,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 * * 2-6",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=[Layer.BRONZE, Interval.DAILY.label],
) as dag:
    fetch_msci_index_data_task = PythonOperator(
        task_id="fetch_msci_index_data",
        python_callable=fetch_msci_indices_data,
        provide_context=True,
        op_kwargs={
            "msci_url_info": MSCI_URL_INFO,
            "msci_index_tmp_file_path": MSCI_INDEX_TMP_FILE_PATH,
            "msci_index_data_s3_key": MSCI_INDEX_DATA_S3_KEY,
        },
    )

    fetch_msci_index_data_task
