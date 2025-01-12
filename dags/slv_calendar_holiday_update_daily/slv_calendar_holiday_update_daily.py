from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from common.constants import AwsConfig, ConnId, Interval, Layer, Owner

S3_BUCKET = Variable.get("s3_bucket")
AWS_REGION = AwsConfig.REGION_NAME
AWS_CONN_ID = ConnId.AWS
GLUE_ROLE = AwsConfig.GLUE_ROLE_NAME


default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="slv_calendar_holiday_update_daily",
    description="캘린더에 휴장일 업데이트",
    default_args=default_args,
    tags=[Layer.SILVER, "market holiday", "triggered", Interval.DAILY.label],
    max_active_runs=1,
) as dag:
    crawl_market_holiday = GlueCrawlerOperator(
        task_id="crawl_market_holiday",
        config={
            "Name": "team3-holiday-crawler",
            "Role": GLUE_ROLE,
            "DatabaseName": "team3-db",
            "Targets": {
                "S3Targets": [
                    {"Path": f"s3://{S3_BUCKET}/bronze/market_holiday/"},
                ]
            },
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    run_holiday_update_glue_job = GlueJobOperator(
        task_id="market_holiday_update_job",
        job_name="slv_calendar_holiday_update_daily",
        script_location=f"s3://{S3_BUCKET}/glue_job_scripts/calendar_holiday_update_glue_job.py",
        script_args={"--current_year": "{{ logical_date.year }}"},
        region_name=AWS_REGION,
        iam_role_name=GLUE_ROLE,
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.2X",
            "NumberOfWorkers": 10,
        },
        update_config=True,
        aws_conn_id=AWS_CONN_ID,
    )

    crawl_market_holiday >> run_holiday_update_glue_job
