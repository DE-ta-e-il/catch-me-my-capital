# This DAG crawls for meta data of all bonds

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.constants import Owner

from brz_bonds_meta_monthly.constants import AirflowParam
from brz_bonds_meta_monthly.extractors import get_metadata

with DAG(
    dag_id="brz_bonds_meta_monthly",
    start_date=AirflowParam.START_DATE.value,
    schedule_interval="0 0 1 * 1-5",
    catchup=False,
    default_args={
        "retries": 1,
        "owner": Owner.DONGWON,
        "retry_delay": timedelta(minutes=1),
    },
    max_active_tasks=2,
    max_active_runs=1,
    tags=["bronze", "bonds metadata", "monthly"],
    description="All Bonds Metadata, States And Corps",
) as dag:
    start_marker = EmptyOperator(task_id="start_of_brz_bonds_meta_tasks")

    # Dynamically generate crawling tasks
    with TaskGroup(group_id="crawler_group") as meta_data_crawler_group:
        # Loops inside the task now
        crawl_for_bonds_metadata = PythonOperator(
            task_id=f"metadata_for_bonds",
            python_callable=get_metadata,
        )
        crawl_for_bonds_metadata

    completion_marker = EmptyOperator(
        task_id="bonds_meta_all_success_check",
    )

    start_marker >> meta_data_crawler_group >> completion_marker
