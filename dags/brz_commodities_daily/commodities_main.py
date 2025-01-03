from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.constants import Interval, Layer, Owner

from brz_commodities_daily.commodities_tasks import CommoditiesPipeline

default_args = {
    "owner": Owner.DAMI,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# NOTE: 가격 데이터를 수집할 종목 코드(티커)에 대한 설명
# - "CL=F": 원유(WTI) 선물
# - "BZ=F": 원유(브렌트유) 선물
# - "GC=F": 금 선물
TICKER_LIST = ["CL=F", "BZ=F", "GC=F"]
S3_BUCKET = Variable.get("s3_bucket")
AWS_CONN_ID = "aws_conn_id"

with DAG(
    dag_id="brz_commodities_daily",
    description="Daily pipeline to acquire and store market data for commodities.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 8, 1),
    catchup=True,
    default_args=default_args,
    tags=[Layer.BRONZE, Interval.DAILY.label, "commodities"],
) as dag:
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    commodities_pipeline = CommoditiesPipeline(TICKER_LIST, S3_BUCKET, s3_hook)

    fetch_commodities_data = PythonOperator(
        task_id="fetch_commodities_data",
        python_callable=commodities_pipeline.run,
    )

    fetch_commodities_data
