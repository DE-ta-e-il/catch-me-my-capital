from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from slv_industry_code_daily.constants import AirflowParam
from slv_industry_code_daily.helpers import to_crawl_or_not_to_crawl

# Some tasks get skipped by the branch operator. 'all-success' rule might break this DAG?
# https://www.marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/#Solving_the_BranchPythonOperator_pitfall
default_args = {
    "owner": "dee",
    "start_date": AirflowParam.START_DATE.value,
    "retries": 0,
    # "retry_delay":
}
must_crawl = AirflowParam.TO_CRAWL.value

with DAG(
    dag_id="slv_industry_code_daily",
    default_args=default_args,
    start_date=datetime(2024, 12, 25),
    schedule_interval="0 0 * * 1-5",
    catchup=False,
    tags=["bronze"],
    max_active_tasks=2,
) as dag:
    # TODO: Is it better to utilize a sub-DAG?
    wait = ExternalTaskSensor(
        task_id="wait_brz_industry_code_daily",
        external_dag_id="brz_industry_code_daily",
        timeout=600,
        mode="reschedule",
    )

    # But should it be a separate DAG?
    task_choice = BranchPythonOperator(
        task_id="divergent_actions_facilitator_lol",
        python_callable=to_crawl_or_not_to_crawl,
        op_args=[
            AirflowParam.TO_CRAWL.value,
            "crawler_group",
            "dummy_lives_matter",
        ],
    )

    with TaskGroup("crawler_group") as crawler_group:
        crawl_for_krx_schema = GlueCrawlerOperator(
            task_id="crawler_krx_industry_codes",
            config={
                "Name": "team3-crawler",
                "Role": "AWSGlueServiceRole-Team3-1",
                "DatabaseName": "team3-db",
                "Targets": {
                    "S3Targets": [
                        {"Path": "s3://team3-1-s3/bronze/industry_code/krx_codes/"}
                    ]
                },
            },
            aws_conn_id="aws_conn_id",
            wait_for_completion=True,
            region_name="ap-northeast-2",
        )

        # Can't I just put all the target paths in the S3Targets?
        crawl_for_gics_schema = GlueCrawlerOperator(
            task_id="crawler_gics_industry_codes",
            config={
                "Name": "team3-crawler",
                "Role": "AWSGlueServiceRole-Team3-1",
                "DatabaseName": "team3-db",
                "Targets": {
                    "S3Targets": [
                        {"Path": "s3://team3-1-s3/bronze/industry_code/gics_codes/"}
                    ]
                },
            },
            aws_conn_id="aws_conn_id",
            wait_for_completion=True,
            region_name="ap-northeast-2",
        )
        crawl_for_krx_schema >> crawl_for_gics_schema

    # For when it is not the time to crawl
    dont_run_crawl = DummyOperator(task_id="dummy_lives_matter")

    run_krx_glue_job = GlueJobOperator(
        task_id="krx_industry_code_job",
        job_name="slv_industry_code_daily",
        script_location="s3://team3-1-s3/glue_job_scripts/industry_code_glue_job.py",
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-Team3-1",
        num_of_dpus=2,
        create_job_kwargs={
            "GlueVersion": "3.0",
            "MaxCapacity": 10,
        },
        aws_conn_id="aws_conn_id",
    )

    success_check = DummyOperator(
        task_id="overall_status_wrapper",
        # Run when its upstream has been skipped or successful
        # NOTE: Downstream of the Branch operator will not run(get skipped) if this is not set!
        trigger_rule="none_failed_min_one_success",
    )

    wait >> task_choice
    task_choice >> crawler_group >> success_check
    task_choice >> dont_run_crawl >> success_check
    success_check >> run_krx_glue_job
