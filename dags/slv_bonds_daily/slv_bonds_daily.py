from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from common.constants import Owner
from slv_bonds_daily.constants import AirflowParam, ProvidersParam, URLParam
from slv_bonds_daily.helpers import to_crawl_or_not_to_crawl

default_args = {
    "owner": Owner.DONGWON,
    "start_date": AirflowParam.START_DATE.value,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Triggered by its upstream DAG, it needed the schedule interval no more, the end!
with DAG(
    dag_id="slv_bonds_daily",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["silver", "bonds", "daily"],
    max_active_tasks=2,
    max_active_runs=1,
) as dag:
    starter = EmptyOperator(task_id="start_of_slv_bonds_daily")

    task_choice = BranchPythonOperator(
        task_id="slv_bonds_brancher",
        python_callable=to_crawl_or_not_to_crawl,
        op_args=[
            AirflowParam.FIRST_RUN.value,
            "slv_bonds_crawler",
            "slv_bonds_skipper",
        ],
    )

    crawl_for_bonds_schema = GlueCrawlerOperator(
        task_id="slv_bonds_crawler",
        config={
            "Name": "Team3-test",
            "Role": "AWSGlueServiceRole-Team3-1",
            "DatabaseName": "team3-db",
            "Targets": {
                "S3Targets": [
                    {"Path": "s3://team3-1-s3/bronze/bonds/"},
                ]
            },
        },
        aws_conn_id="aws_conn_id",
        wait_for_completion=True,
        region_name="ap-northeast-2",
    )

    dont_run_crawl = EmptyOperator(task_id="slv_bonds_skipper")

    run_bonds_job = GlueJobOperator(
        task_id="slv_bonds_glue_job",
        job_name="team3_slv_bonds_daily",
        script_location="s3://team3-1-s3/glue_job_scripts/bonds_glue_job.py",
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-Team3-1",
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.2X",
            "NumberOfWorkers": 10,
        },
        update_config=True,
        aws_conn_id="aws_conn_id",
    )

    success_check = EmptyOperator(
        task_id="slv_bonds_status_wrapper",
        trigger_rule="none_failed_min_one_success",
    )

    starter >> task_choice
    task_choice >> crawl_for_bonds_schema >> success_check
    task_choice >> dont_run_crawl >> success_check
    success_check >> run_bonds_job
