from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.sensors.external_task import ExternalTaskSensor
from common.config_utils import AwsConfig
from common.constants import ConnId, Interval, Layer, Owner
from slv_commodities_daily.query import INITIALIZE_QUERY, SWAP_TABLE_QUERY

# TODO: 공통 args 정리 필요
default_args = {
    "owner": Owner.DAMI,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

FEATURE = "commodities"
TARGET_TABLE = "fact_commodities_price"

with DAG(
    dag_id="slv_commodities_daily",
    description="Daily pipeline to transform market data for commodities.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.SILVER, Interval.DAILY.label, FEATURE],
) as dag:
    wait_for_fetch_commodities_data = ExternalTaskSensor(
        task_id="wait_for_fetch_commodities_data",
        external_dag_id="brz_commodities_daily",
        external_task_id="fetch_commodities_data",
        timeout=600,
        allowed_states=["success", "skipped"],
        mode="reschedule",
    )

    initialize_tables = RedshiftDataOperator(
        task_id="initialize_tables",
        aws_conn_id=ConnId.AWS,
        cluster_identifier=AwsConfig.REDSHIFT_CLUSTER_ID,
        database=AwsConfig.REDSHIFT_DB_NAME,
        db_user=AwsConfig.REDSHIFT_DB_USER,
        sql=INITIALIZE_QUERY.format(target_table=TARGET_TABLE),
    )

    transform_commodities_data = GlueJobOperator(
        task_id="transform_commodities_data",
        job_name=AwsConfig.GLUE_JOB_NAME_TEMPLATE.format(feature=FEATURE),
        aws_conn_id=ConnId.AWS,
        iam_role_name=AwsConfig.GLUE_IAM_ROLE_NAME,
        script_location=f"s3://{AwsConfig.S3_BUCKET_NAME}/glue_job_scripts/{FEATURE}_glue_job.py",
        create_job_kwargs=AwsConfig.GLUE_JOB_DEFAULT_KWARGS,
        update_config=True,
        script_args={
            "--s3_bucket": AwsConfig.S3_BUCKET_NAME,
            "--feature": FEATURE,
            "--redshift_url": AwsConfig.REDSHIFT_URL,
            "--target_table": f"staging.{TARGET_TABLE}",
        },
    )

    swap_tables = RedshiftDataOperator(
        task_id="swap_tables",
        aws_conn_id=ConnId.AWS,
        cluster_identifier=AwsConfig.REDSHIFT_CLUSTER_ID,
        database=AwsConfig.REDSHIFT_DB_NAME,
        db_user=AwsConfig.REDSHIFT_DB_USER,
        sql=SWAP_TABLE_QUERY.format(target_table=TARGET_TABLE),
    )

    (
        wait_for_fetch_commodities_data
        >> initialize_tables
        >> transform_commodities_data
        >> swap_tables
    )
