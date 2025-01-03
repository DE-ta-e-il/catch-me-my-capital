from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from brz_kr_etf_daily.tasks import (
    fetch_etf_from_krx_api_to_s3,
    verify_market_open,
)

default_args = {
    "owner": "j-eum",  # TODO: 공통 ENUM적용 예정
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="brz_kr_etf_daily",
    default_args=default_args,
    description="한국거래소 ETF 종목별 시세",
    tags=["bronze", "ETF", "daily", "weekday"],
    schedule="0 5 * * 1-5",
    start_date=datetime(2025, 12, 20),  # 과거 데이터는 별도로 한번에 처리
    catchup=True,
    max_active_runs=3,
) as dag:
    verify_market_open = ShortCircuitOperator(
        task_id="verify_market_open",
        python_callable=verify_market_open,
    )

    fetch_etf_from_krx_api_to_s3 = PythonOperator(
        task_id="fetch_etf_krx_api",
        python_callable=fetch_etf_from_krx_api_to_s3,
    )

    verify_market_open >> fetch_etf_from_krx_api_to_s3
