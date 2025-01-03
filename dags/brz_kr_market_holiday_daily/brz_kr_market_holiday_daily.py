from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import Owner

from brz_kr_market_holiday_daily.tasks import (
    fetch_krx_market_holiday_to_s3,
)

default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="brz_kr_market_holiday_daily",
    description="국내 주식 휴장일",
    schedule="@daily",
    start_date=datetime(2024, 12, 20),
    default_args=default_args,
    tags=["bronze", "market holiday", "daily"],
    catchup=True,
    max_active_runs=3,
) as dag:
    fetch_krx_market_holiday_to_s3 = PythonOperator(
        task_id="fetch_kr_market_holiday_to_s3",
        python_callable=fetch_krx_market_holiday_to_s3,
    )
