from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup
from common.constants import Interval, Layer, Owner

default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="slv_kr_market_holiday_daily",
    description="캘린더에 휴장일 업데이트",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    tags=[Layer.SILVER, "market holiday", Interval.DAILY.label],
    catchup=True,
    max_active_runs=1,
) as dag:
    # wait_for_brz_holiday_update = S3KeySensor(
    #     bucket_name=ProvidersParam.S3_BUCKET.value,
    #     bucket_key="bronze/kr_market_holiday/year={{ logical_date.year }}/data.json",
    #     poke_interval=60,
    #     timeout=600,
    #     aws_conn_id="aws_conn_id",
    #     task_id="wait_brz_market_holiday_daily",
    #     mode="reschedule",  # poke mode takes up a worker slot while waiting.
    # )

    # # crawl_market_holiday = GlueCrawlerOperator(
    #     task_id="crawler_market_holiday",
    #     config={
    #         "Name": "Team3-test",
    #         "Role": "AWSGlueServiceRole-Team3-1",
    #         "DatabaseName": "team3-db",
    #         "Targets": {
    #             "S3Targets": [
    #                 {"Path": "s3://team3-1-s3/bronze/market_holiday/"},
    #             ]
    #         },
    #     },
    #     aws_conn_id="aws_conn_id",
    #     wait_for_completion=True,
    #     region_name="ap-northeast-2",
    # )

    run_holiday_update_glue_job = GlueJobOperator(
        task_id="market_holiday_update_job",
        job_name="slv_calendar_holiday_update_daily",
        # script_location="s3://team3-1-s3/glue_job_scripts_test/market_holiday_update_job.py",
        script_location="s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/team3-calendar-holiday.py",
        script_args={"--current_year": "{{ logical_date.year }}"},
        region_name="ap-northeast-2",
        iam_role_name="AWSGlueServiceRole-Team3-1",
        num_of_dpus=2,
        create_job_kwargs={
            "GlueVersion": "3.0",
            "MaxCapacity": 10,
        },
        aws_conn_id="aws_conn_id",
    )

    # success_check = DummyOperator(
    #     task_id="overall_status_wrapper",
    #     # Run when its upstream has been skipped or successful
    #     # NOTE: Downstream of the Branch operator will not run(get skipped) if this is not set!
    #     trigger_rule="none_failed_min_one_success",
    # )

    # wait >> task_choice
    # task_choice >> crawler_group >> success_check
    # task_choice >> dont_run_crawl >> success_check
    # success_check >> run_krx_glue_job

    # wait_for_brz_holiday_update >> run_holiday_update_glue_job
