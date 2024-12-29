# This DAG crawls for meta data of all bonds

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from brz_bonds_meta_monthly.brz_bonds_meta_constants import START_DATE
from brz_bonds_meta_monthly.brz_bonds_meta_extractors import (
    get_categories,
    get_metadata,
)

with DAG(
    dag_id="brz_govt_bonds_meta_monthly",
    start_date=START_DATE,
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
        urls_dict = get_categories()
        for category in urls_dict:
            for bond_name in urls_dict[category]:
                crawl_for_bonds_metadata = PythonOperator(
                    task_id=f"metadata_for_{category}_{bond_name}",
                    python_callable=get_metadata,
                    op_args=[category, bond_name],
                )
                crawl_for_bonds_metadata

    meta_data_crawler_group
