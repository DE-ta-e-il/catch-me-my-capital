from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.bronze.constants import FIRST_RUN, START_DATE, URLS_DICT
from plugins.bronze.extractors import generate_urls, get_bond_data

with DAG(
    dag_id="brz_bonds_day",
    start_date=START_DATE,
    schedule_interval="0 0 * * *",
    catchup=not FIRST_RUN,
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

    with TaskGroup(group_id="api_caller_group") as api_caller_group:
        for bond_kind in URLS_DICT:
            get_corresponding_bond_data = PythonOperator(
                task_id=f"fetch_{bond_kind}",
                python_callable=get_bond_data,
                provide_context=True,
                op_args=[bond_kind],
            )
            get_corresponding_bond_data

    url_generator >> api_caller_group
