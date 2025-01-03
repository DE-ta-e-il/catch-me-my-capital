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
    dag_id="brz_economic_indicators_yearly",
    description="Yearly pipeline to acquire and store economic indicators.",
    # 한국은행에서 제공하는 연간 경제지표는 통상적으로 4월 중순-말에 공개되기 때문에,
    # 5월 1일에는 안정적인 데이터 수집이 가능해질 것으로 보고, 아래와 같이 스케쥴을 설정하였습니다.
    schedule_interval="0 0 1 5 *",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.BRONZE, Interval.YEARLY.label, "economic_indicators"],
) as dag:
    start, end = EmptyOperator(task_id="start"), EmptyOperator(task_id="end")

    tasks = [
        BankOfKoreaOperator(
            task_id=f"fetch_{stat_name.lower()}",
            op_kwargs={
                "interval": Interval.YEARLY.name,
                "stat_name": stat_name,
            },
        )
        for stat_name in STAT_NAME_LIST
    ]

    start >> tasks >> end
