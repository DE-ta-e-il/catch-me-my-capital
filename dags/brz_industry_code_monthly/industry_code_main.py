# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.
# TODO: Use airflow.models.connection to manage connections once AWS secrets manager is utilized.

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.constants import MARKETS
from common.extractors import crawl_industry_codes, fetch_industry_codes
from common.uploaders import upload_codes_to_s3

with DAG(
    dag_id="brz_industry_code_month",
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    tags=["bronze"],
    description="A DAG that fetches industry(sector) codes for stocks.",
    default_args={"retries": 0, "trigger_rule": "all_success", "owner": "dee"},
    max_active_tasks=3,
) as dag:
    with TaskGroup("kospi_kosdaq_codes_task_group") as kospi_kosdaq_group:
        markets = MARKETS

        # previous = None # Pairs are not dependant anymore
        for market, codes in markets.items():
            start_of_task_pair = DummyOperator(task_id=f"start_of_task_pair_{market}")
            end_of_task_pair = DummyOperator(task_id=f"end_of_task_pair_{market}")

            codes_fetcher = PythonOperator(
                task_id=f"{market}_industry_codes",
                python_callable=fetch_industry_codes,
                op_args=[market, codes[0], codes[1]],
            )

            uploader = PythonOperator(
                task_id=f"upload_{market}_codes",
                python_callable=upload_codes_to_s3,
                op_args=[market],
            )

            # Pairs will run parallel
            start_of_task_pair >> codes_fetcher >> uploader >> end_of_task_pair

            # if previous: # Pairs are not dependant anymore
            #     previous >> codes_fetcher
            # previous = uploader

    gics_codes_fetcher = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=crawl_industry_codes,
    )

    gics_uploader = PythonOperator(
        task_id="upload_gics_codes",
        python_callable=upload_codes_to_s3,
        op_args=["gics"],
    )

    # Max active tasks needed = 3 😨 I have faith in my rig!
    kospi_kosdaq_group
    gics_codes_fetcher >> gics_uploader
