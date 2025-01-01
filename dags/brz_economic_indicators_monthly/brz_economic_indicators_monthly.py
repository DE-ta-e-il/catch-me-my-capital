from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.bank_of_korea_constants import Stat
from common.constants import Interval, Layer, Owner
from operators.bank_of_korea_operator import BankOfKoreaOperator

# TODO: config 값 확인
default_args = {
    "owner": Owner.DAMI,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# NOTE: 월 단위로 수집할 경제 지표 목록
STAT_NAME_LIST = [Stat.CENTRAL_BANK_POLICY_RATES.name]
INTERVAL_NAME = Interval.MONTHLY.name

with DAG(
    dag_id="brz_economic_indicators_monthly",
    description="MONTHLY pipeline to acquire and store economic indicators.",
    schedule_interval="0 0 15 * *",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.BRONZE, Interval.MONTHLY.label, "economic_indicators"],
) as dag:
    start, end = EmptyOperator(task_id="start"), EmptyOperator(task_id="end")

    tasks = [
        BankOfKoreaOperator(
            task_id=f"fetch_{stat_name.lower()}",
            op_kwargs={
                "interval": INTERVAL_NAME,
                "stat_name": stat_name,
            },
        )
        for stat_name in STAT_NAME_LIST
    ]

    start >> tasks >> end
