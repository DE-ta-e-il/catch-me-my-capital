from datetime import datetime, timedelta

from airflow import DAG
from common.constants import Interval, Layer, Owner, Redshift
from operators.yfinance_operator import YFinanceOperator

SCHEMA = Redshift.SchemaName.SILVER
TABLE = Redshift.TableName.DIM_INDUSTRY_CODE
CATEGORY = "kr_stock"

default_args = {
    "owner": Owner.JUNGMIN,
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="brz_kr_stock_daily",
    default_args=default_args,
    description="KOSPI/KOSDAQ 상장 주식",
    tags=[Layer.BRONZE, "KOSPI", "KOSDAQ", Interval.DAILY.label],
    schedule="0 5 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=5,
) as dag:
    fetch_kr_stock_data = YFinanceOperator(
        task_id="fetch_kr_stock_data",
        category=CATEGORY,
        query=f"""
            SELECT item_code, market
            FROM {SCHEMA}.{TABLE}
            WHERE issue_date = %s
            """,
    )
