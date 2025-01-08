import json
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.sensors.external_task import ExternalTaskSensor
from common.bank_of_korea_constants import Stat
from common.bank_of_korea_constants_utils import generate_table_names
from common.config_utils import AwsConfig
from common.constants import ConnId, Interval, Layer, Owner

# TODO: 공통 args 정리 필요
default_args = {
    "owner": Owner.DAMI,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# TODO: 브론즈에서 아래 부분을 바꾸면 이 부분도 변경해야 하기 때문에 효율적으로 상수를 관리하기 위한 전략이 필요합니다.
FEATURE = "economic_indicators"
DB_SCHEMA = "silver"
STAT_NAME_LIST = [Stat.GDP_GROWTH_RATE.name]
source_tables = generate_table_names(Interval.YEARLY.label, STAT_NAME_LIST)

with DAG(
    dag_id="slv_economic_indicators_yearly",
    description="Yearly pipeline to transform economic indicators.",
    schedule_interval="0 0 1 5 *",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.SILVER, Interval.YEARLY.label, FEATURE],
    template_searchpath=["/opt/airflow/dags/common/sql"],
) as dag:
    economic_indicators_sensors = [
        ExternalTaskSensor(
            task_id=f"wait_for_{stat_name.lower()}",
            external_dag_id="brz_economic_indicators_yearly",
            external_task_id=f"fetch_{stat_name.lower()}",
            timeout=600,
            allowed_states=["success", "skipped"],
            mode="reschedule",
        )
        for stat_name in STAT_NAME_LIST
    ]

    initialize_tables = RedshiftDataOperator(
        task_id="initialize_tables",
        aws_conn_id=ConnId.AWS,
        cluster_identifier=AwsConfig.REDSHIFT_CLUSTER_ID,
        database=AwsConfig.REDSHIFT_DB_NAME,
        db_user=AwsConfig.REDSHIFT_DB_USER,
        sql="initialize_tables.sql",
    )

    transform_economic_indicators_data = GlueJobOperator(
        task_id="transform_economic_indicators_data",
        job_name=AwsConfig.GLUE_JOB_NAME_TEMPLATE.format(feature=FEATURE),
        aws_conn_id=ConnId.AWS,
        iam_role_name=AwsConfig.GLUE_IAM_ROLE_NAME,
        script_location=f"s3://{AwsConfig.S3_BUCKET_NAME}/glue_job_scripts/{FEATURE}_glue_job.py",
        create_job_kwargs=AwsConfig.GLUE_JOB_DEFAULT_KWARGS,
        update_config=True,
        script_args={
            "--s3_bucket": AwsConfig.S3_BUCKET_NAME,
            "--redshift_url": AwsConfig.REDSHIFT_URL,
            "--db_schema": DB_SCHEMA,
            "--source_tables": json.dumps(source_tables),
        },
    )

    remove_duplicates_in_tables = RedshiftDataOperator(
        task_id="remove_duplicates",
        aws_conn_id=ConnId.AWS,
        cluster_identifier=AwsConfig.REDSHIFT_CLUSTER_ID,
        database=AwsConfig.REDSHIFT_DB_NAME,
        db_user=AwsConfig.REDSHIFT_DB_USER,
        sql="remove_duplicates.sql",
    )

    (
        economic_indicators_sensors
        >> initialize_tables
        >> transform_economic_indicators_data
        >> remove_duplicates_in_tables
    )
