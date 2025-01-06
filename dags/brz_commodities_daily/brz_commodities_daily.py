from datetime import timedelta

import pendulum
from airflow import DAG
from common.constants import Interval, Layer, Owner
from operators.yfinance_operator import YFinanceOperator

default_args = {
    "owner": Owner.DAMI,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# NOTE: 가격 데이터를 수집할 종목 코드(티커)에 대한 설명
# - "CL=F": 원유(WTI) 선물
# - "BZ=F": 원유(브렌트유) 선물
# - "GC=F": 금 선물
TICKER_LIST = ["CL=F", "BZ=F", "GC=F"]
CATEGORY = "commodities"

with DAG(
    dag_id="brz_commodities_daily",
    description="Daily pipeline to acquire and store market data for commodities.",
    schedule="@daily",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.BRONZE, Interval.DAILY.label, CATEGORY],
) as dag:
    fetch_commodities_data = YFinanceOperator(
        task_id="fetch_commodities_data", category=CATEGORY, tickers=TICKER_LIST
    )
