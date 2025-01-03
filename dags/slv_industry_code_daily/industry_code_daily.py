from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    dag_id="slv_coin_daily",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bronze"],
) as dag:
    wait = ExternalTaskSensor(
        task_id="wait_brz_industry_code_daily",
        external_dag_id="brz_industry_code_daily",
        external_task_group_id="kospi_kosdaq_codes_task_group",
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
    )

    # But should it be a separate DAG?
    task_choice = BranchPythonOperator(
        task_id="divergent_actions_facilitator_lol",
        python_callable=to_crawl_or_not_to_crawl,
        op_args=[
            AirflowParam.TO_CRAWL.value,
            "crawler_industry_codes",
            "dummy_lives_matter",
        ],
    )

    crawl_for_schema = GlueCrawlerOperator(
        task_id="crawler_industry_codes",
        config={
            "Name": "team3-industry-code-crawler",
            "Role": "AWSGlueServiceRole-Team3-1",
            "DatabaseName": "team3-db",
            "Targets": {
                "S3Targets": [{"Path": "s3://team3-1-s3/bronze/industry_code/"}]
            },
        },
        aws_conn_id="aws_conn_id",
        wait_for_completion=True,
        region_name="ap-northeast-2",
    )

    # For when it is not the time to crawl
    dont_run_crawl = DummyOperator(task_id="dummy_lives_matter")

    run_glue_job = GlueJobOperator(
        task_id="industry_code_job",
        job_name="slv_industry_code_daily",
        script_location="s3://team3-1-s3/glue_job_scripts/industry_code_job.py",
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-Team3-1",
        num_of_dpus=2,
        create_job_kwargs={
            "GlueVersion": "3.0",
            "MaxCapacity": 10,
        },
        aws_conn_id="aws_conn_id",
        # NOTE: Downstream of the Branch operator will not run(get skipped) if this is not set!
        trigger_rule="none_failed_min_one_success",
    )

    wait >> task_choice >> [crawl_for_schema, dont_run_crawl] >> run_glue_job
