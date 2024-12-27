# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.
# TODO: Use airflow.models.connection to manage connections once AWS secrets manager is utilized.

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.bronze.pyops_industry_codes import (
    crawl_industry_codes,
    fetch_industry_codes,
    upload_to_s3,
)

with DAG(
    dag_id="brz_industry_code_month",
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    tags=["bronze"],
    description="A DAG that fetches industry(sector) codes for stocks.",
    default_args={"retries": 0, "trigger_rule": "all_success", "owner": "dee"},
    max_active_tasks=1,
) as dag:
    with TaskGroup("kospi_kosdaq_codes_task_group") as kospi_kosdaq_group:
        markets = {
            "kospi": ["MDC0201020101", "STK"],
            "kosdaq": ["MDC0201020506", "KSQ"],
        }

        previous = None
        for market, codes in markets.items():
            codes_fetcher = PythonOperator(
                task_id=f"{market}_industry_codes",
                python_callable=fetch_industry_codes,
                op_args=[market, codes[0], codes[1]],
            )

            uploader = PythonOperator(
                task_id=f"upload_{market}_codes",
                python_callable=upload_to_s3,
                op_args=[market],
            )
            # inner dependency
            codes_fetcher >> uploader

            if previous:
                previous >> codes_fetcher
            previous = uploader

    gics_codes_fetcher = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=crawl_industry_codes,
    )

    gics_uploader = PythonOperator(
        task_id="upload_gics_codes",
        python_callable=upload_to_s3,
        op_args=["gics"],
    )

    kospi_kosdaq_group >> gics_codes_fetcher >> gics_uploader
