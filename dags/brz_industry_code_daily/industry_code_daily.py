# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.
# TODO: Use airflow.models.connection to manage connections once AWS secrets manager is utilized.

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from brz_industry_code_daily.constants import MARKETS
from brz_industry_code_daily.extractors import (
    crawl_industry_codes,
    fetch_industry_codes,
)

with DAG(
    dag_id="brz_industry_code_daily",
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 0 * * 1-5",
    catchup=False,
    tags=["bronze"],
    description="A DAG that fetches industry(sector) codes for stocks.",
    default_args={"retries": 0, "trigger_rule": "all_success", "owner": "dee"},
    max_active_tasks=3,
) as dag:
    with TaskGroup("kospi_kosdaq_codes_task_group") as kospi_kosdaq_group:
        markets = MARKETS

        for market, codes in markets.items():
            krx_codes_fetcher = PythonOperator(
                task_id=f"{market}_industry_codes",
                python_callable=fetch_industry_codes,
                op_args=[market, codes[0], codes[1]],
            )

            krx_codes_fetcher

    gics_codes_fetcher = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=crawl_industry_codes,
    )

    # Max active tasks needed = 3 😨 I have faith in my rig!
    kospi_kosdaq_group
    gics_codes_fetcher
