# This DAG crawls for meta data of all bonds

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import plugins.bronze.constants as C
from plugins.bronze.pyops_extractors import get_categories, get_metadata

with DAG(
    dag_id="brz_govt_bonds_meta_month",
    start_date=C.START_DATE,
    schedule_interval="0 0 1 * *",
    catchup=False,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
        "owner": "dee",
    },
    max_active_tasks=2,
    tags=["bronze"],
    description="All Bonds Metadata, States And Corps",
) as dag:
    # Dynamically generate crawling tasks
    with TaskGroup(group_id="crawler_group") as meta_data_crawler_group:
        # Put prev_task right below category loop to parallelize
        prev_task = None
        urls_dict = get_categories()
        for category in urls_dict:
            for bond_name in urls_dict[category]:
                curr_task = PythonOperator(
                    task_id=f"{category}_{bond_name}",
                    python_callable=get_metadata,
                    op_args=[category, bond_name],
                )
                if prev_task:
                    prev_task >> curr_task
                prev_task = curr_task

    meta_data_crawler_group
