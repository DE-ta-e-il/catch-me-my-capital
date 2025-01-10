from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dbt_run_test",
    tags=["test"],
    default_args={},
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    bash_test = BashOperator(
        task_id="dbt_task_test",
        bash_command=(
            f"""
                cd /opt/airflow/dags/dbt/cmmc && dbt run
            """
        ),
    )
