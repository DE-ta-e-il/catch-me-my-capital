from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from common.constants import Interval, Layer, Owner

from slv_generate_calendar_yearly.sql import *

default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="slv_generate_calendar_yearly",
    description="내년 기본 캘린더 생성",
    schedule="0 0 1 12 *",
    start_date=datetime(2014, 12, 1),
    default_args=default_args,
    tags=[Layer.SILVER, "calender", Interval.YEARLY.label, "Redshift"],
    catchup=True,
    max_active_runs=1,
) as dag:
    create_calendar_table = RedshiftDataOperator(
        task_id="create_calendar_table",
        aws_conn_id="aws_conn_id",
        cluster_identifier=Variable.get("redshift_cluster_identifier"),
        database=Variable.get("redshift_cluster_db_name"),
        db_user=Variable.get("redshift_cluster_master_user"),
        sql=CREATE_CALENDAR_TABLE_SQL,
    )

    create_calendar_procedure = RedshiftDataOperator(
        task_id="create_calendar_procedure",
        aws_conn_id="aws_conn_id",
        cluster_identifier=Variable.get("redshift_cluster_identifier"),
        database=Variable.get("redshift_cluster_db_name"),
        db_user=Variable.get("redshift_cluster_master_user"),
        sql=CREATE_CALENDAR_PRECEDURE_SQL,
    )

    generate_next_year_calendar = RedshiftDataOperator(
        task_id="generate_next_year_calendar",
        aws_conn_id="aws_conn_id",
        cluster_identifier=Variable.get("redshift_cluster_identifier"),
        database=Variable.get("redshift_cluster_db_name"),
        db_user=Variable.get("redshift_cluster_master_user"),
        sql=f"CALL {SCHEMA}.{CALENDER_PROCEDURE_NAME}('{{{{ logical_date.year + 1 }}}}');",
    )

    create_calendar_table >> create_calendar_procedure >> generate_next_year_calendar
