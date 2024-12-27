from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import plugins.bronze.constants as C
from plugins.bronze.pyops_extractors import generate_urls, get_bond_data

with DAG(
    dag_id="brz_bonds_day",
    start_date=C.START_DATE,
    schedule_interval="0 0 * * *",
    catchup=not C.FIRST_RUN,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
        "owner": "dee",
    },
    max_active_tasks=2,
    tags=["bronze"],
    description="Bonds of Korea and US, State and Corporate. Expandable.",
) as dag:
    # Generates full url from parameters
    url_generator = PythonOperator(
        task_id="bonds_url_generator",
        python_callable=generate_urls,
        provide_context=True,
    )

    # bond_kind tasks can parallelize
    with TaskGroup(group_id="api_caller_group") as api_caller_group:
        prev_task = None
        id = 0
        for bond_kind in C.URLS_DICT:
            curr_task = PythonOperator(
                task_id=f"fetch_{bond_kind}_{id}",
                python_callable=get_bond_data,
                provide_context=True,
                op_args=[bond_kind],
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task
            id += 1

    url_generator >> api_caller_group
