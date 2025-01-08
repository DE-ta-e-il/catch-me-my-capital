from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from common.constants import Interval, Layer, Owner, Redshift

from brz_kr_etf_daily.tasks import (
    fetch_etf_from_krx_api_to_s3,
    verify_market_open,
)

AWS_CONN_ID = "aws_conn_id"
REDSHIFT_CLUSTER = Variable.get("redshift_cluster_identifier")
REDSHIFT_DB = Variable.get("redshift_cluster_db_name")
REDSHIFT_USER = Variable.get("redshift_cluster_master_user")

SCHEMA = Redshift.SchemaName.SILVER
TABLE = Redshift.TableName.DIM_CALENDAR
GET_WORKDAY_SQL = f"""
    SELECT
        dc."date" AS today_date,
        dc.is_market_holiday AS today_is_holiday,
        (
            SELECT MAX(dc_sub."date")
            FROM {SCHEMA}.{TABLE} dc_sub
            WHERE dc_sub."date" < dc."date" 
                AND dc_sub.is_market_holiday = False) AS previous_working_day
    FROM {SCHEMA}.{TABLE} dc
    WHERE dc."date" = :current_date
    ;
    """

default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="brz_kr_etf_daily",
    default_args=default_args,
    description="한국거래소 ETF 종목별 시세",
    tags=[Layer.BRONZE, "ETF", Interval.DAILY.label, "weekday"],
    schedule="0 5 * * 1-5",
    start_date=datetime(2020, 1, 3),
    catchup=True,
    max_active_runs=5,
) as dag:
    get_workday_info = RedshiftDataOperator(
        task_id="get_workday_info",
        aws_conn_id=AWS_CONN_ID,
        cluster_identifier=REDSHIFT_CLUSTER,
        database=REDSHIFT_DB,
        db_user=REDSHIFT_USER,
        sql=GET_WORKDAY_SQL,
        parameters=[{"name": "current_date", "value": "{{ ds }}"}],
        return_sql_result=True,
    )

    verify_market_open = ShortCircuitOperator(
        task_id="verify_market_open",
        python_callable=verify_market_open,
    )

    fetch_etf_from_krx_api_to_s3 = PythonOperator(
        task_id="fetch_etf_krx_api",
        python_callable=fetch_etf_from_krx_api_to_s3,
    )

    get_workday_info >> verify_market_open >> fetch_etf_from_krx_api_to_s3
