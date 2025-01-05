"""
DEPRECATED: This DAG is no longer in use as of 2020-01-01.
It has been replaced by 'brz_kr_etf_daily', which performs the same operations.
Please remove any references to this DAG and use 'brz_kr_etf_daily' going forward.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from common.constants import Interval, Layer, Owner

from brz_kr_etf_daily.tasks_deprecated import (
    fetch_etf_from_krx_web_to_s3,
    verify_market_open,
)

default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="brz_kr_etf_daily_deprecated",
    default_args=default_args,
    description="한국거래소 ETF 종목별 시세",
    tags=[Layer.BRONZE, "ETF", Interval.DAILY.label, "weekday"],
    schedule="0 0 * * 1-5",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2019, 12, 31),
    catchup=True,
    max_active_runs=3,
) as dag:
    verify_market_open = ShortCircuitOperator(
        task_id="verify_market_open",
        python_callable=verify_market_open,
    )

    fetch_etf_from_krx_web_to_s3 = PythonOperator(
        task_id="fetch_etf_krx_web",
        python_callable=fetch_etf_from_krx_web_to_s3,
    )

    verify_market_open >> fetch_etf_from_krx_web_to_s3
