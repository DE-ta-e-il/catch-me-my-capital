from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup
from common.constants import Owner

from slv_bonds_meta_monthly.constants import AirflowParam, ProvidersParam, URLParam
from slv_bonds_meta_monthly.helpers import to_crawl_or_not_to_crawl

default_args = {
    "owner": Owner.DONGWON,
    "start_date": AirflowParam.BONDS_META_START_DATE.value,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="slv_bonds_meta_monthly",
    default_args=default_args,
    schedule_interval="0 0 1 * 1-5",
    catchup=False,
    tags=["silver", "bonds", "monthly", "meta"],
    max_active_tasks=2,
    max_active_runs=1,
) as dag:
    starter = EmptyOperator(task_id="start_of_slv_bonds_meta_daily")

    with TaskGroup("slv_bonds_meta_sensor_group") as sensor_group:
        wait = S3KeySensor(
            bucket_name=ProvidersParam.S3_BUCKET.value,
            bucket_key="bronze/bonds_meta/ymd={{ ds }}/bonds_meta_{{ execution_date.strftime('%Y-%m') }}.json",
            poke_interval=60,
            timeout=600,
            aws_conn_id="aws_conn_id",
            task_id=f"wait_for_brz_bonds_meta",
            mode="reschedule",
        )
        wait

    group_success_check = EmptyOperator(
        task_id="slv_bonds_meta_sensor_group_completion_check"
    )

    task_choice = BranchPythonOperator(
        task_id="slv_bonds_meta_brancher",
        python_callable=to_crawl_or_not_to_crawl,
        op_args=[
            AirflowParam.FIRST_RUN.value,
            "slv_bonds_meta_crawler",
            "slv_bonds_meta_skipper",
        ],
    )

    crawl_for_bonds_schema = GlueCrawlerOperator(
        task_id="slv_bonds_meta_crawler",
        config={
            "Name": "Team3-test",
            "Role": "AWSGlueServiceRole-Team3-1",
            "DatabaseName": "team3-db",
            "Targets": {"S3Targets": [{"Path": "s3://team3-1-s3/bronze/bonds_meta/"}]},
        },
        aws_conn_id="aws_conn_id",
        wait_for_completion=True,
        region_name="ap-northeast-2",
    )

    dont_run_crawl = EmptyOperator(task_id="slv_bonds_meta_skipper")

    run_bonds_job = GlueJobOperator(
        task_id="slv_bonds_meta_glue_job",
        job_name="slv_bonds_meta_monthly",
        script_location="s3://team3-1-s3/glue_job_scripts/bonds_meta_glue_job.py",
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-Team3-1",
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.2X",
            "NumberOfWorkers": 10,
        },
        aws_conn_id="aws_conn_id",
    )

    success_check = EmptyOperator(
        task_id="slv_bonds_meta_status_wrapper",
        trigger_rule="none_failed_min_one_success",
    )

    starter >> sensor_group >> group_success_check >> task_choice
    task_choice >> crawl_for_bonds_schema >> success_check
    task_choice >> dont_run_crawl >> success_check
    success_check >> run_bonds_job
