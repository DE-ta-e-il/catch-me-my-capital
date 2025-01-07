# TODO: Check the silver layer notion page https://www.notion.so/Silver-Layer-DB-84d715eb2a02479b8c60ba68bce09856
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.constants import Owner

from brz_bonds_daily.constants import AirflowParam, URLParam
from brz_bonds_daily.extractors import generate_urls, get_bond_data

with DAG(
    dag_id="brz_bonds_daily",
    start_date=AirflowParam.START_DATE.value,
    schedule_interval="0 0 * * 1-5",
    catchup=False,
    default_args={
        "retries": 1,
        "owner": Owner.DONGWON,
        "retry_delay": timedelta(minutes=1),
    },
    max_active_tasks=4,
    max_active_runs=1,
    tags=["bronze", "bonds", "daily"],
    description="Bonds of Korea and US, State and Corporate. Expandable.",
) as dag:
    # Generates full url from parameters
    url_generator = PythonOperator(
        task_id="bonds_url_generator",
        python_callable=generate_urls,
        provide_context=True,
    )

    with TaskGroup(group_id="api_caller_group") as api_caller_group:
        for bond_kind in URLParam.URLS_DICT.value:
            get_corresponding_bond_data = PythonOperator(
                task_id=f"fetch_{bond_kind}",
                python_callable=get_bond_data,
                provide_context=True,
                op_args=[bond_kind],
            )
            get_corresponding_bond_data

    completion_marker = EmptyOperator(
        task_id="bonds_all_success_check",
    )

    url_generator >> api_caller_group >> completion_marker
