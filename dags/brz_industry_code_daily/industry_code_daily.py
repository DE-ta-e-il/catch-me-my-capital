# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.
# TODO: Use airflow.models.connection to manage connections once AWS secrets manager is utilized.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from common.constants import Owner

from brz_industry_code_daily.extractors import (
    crawl_industry_codes,
    fetch_industry_codes,
)

with DAG(
    dag_id="brz_industry_code_daily",
    start_date=datetime(2015, 1, 1),
    schedule_interval="0 0 * * 1-5",
    catchup=True,
    tags=["bronze", "industry_code", "daily"],
    description="A DAG that fetches industry(sector) codes for stocks.",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "owner": Owner.DONGWON,
    },
    max_active_tasks=3,
) as dag:
    krx_codes_fetcher = PythonOperator(
        task_id=f"krx_industry_codes",
        python_callable=fetch_industry_codes,
    )

    gics_codes_fetcher = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=crawl_industry_codes,
    )

    clearsky = DummyOperator(
        task_id="task_completion_wrapper",
    )

    # Max active tasks needed = 3 😨 I have faith in my rig!
    krx_codes_fetcher
    gics_codes_fetcher
    [krx_codes_fetcher, gics_codes_fetcher] >> clearsky
