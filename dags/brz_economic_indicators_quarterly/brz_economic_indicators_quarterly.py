from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common.bank_of_korea_constants import Stat
from common.constants import Interval, Layer, Owner
from operators.bank_of_korea_operator import BankOfKoreaOperator

default_args = {
    "owner": Owner.DAMI,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# NOTE: 새로운 경제 지표가 추가 수집될 가능성이 있어, 리스트 형태로 구현해두었습니다.
STAT_NAME_LIST = [Stat.GDP_GROWTH_RATE.name]

with DAG(
    dag_id="brz_economic_indicators_quarterly",
    description="QUARTERLY pipeline to acquire and store economic indicators.",
    schedule_interval="0 0 15 */3 *",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.BRONZE, Interval.QUARTERLY.label, "economic_indicators"],
) as dag:
    start, end = EmptyOperator(task_id="start"), EmptyOperator(task_id="end")

    tasks = [
        BankOfKoreaOperator(
            task_id=f"fetch_{stat_name.lower()}",
            op_kwargs={
                "interval": Interval.QUARTERLY.name,
                "stat_name": stat_name,
            },
        )
        for stat_name in STAT_NAME_LIST
    ]

    start >> tasks >> end
