from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import (
    GlueCrawlerOperator,
    GlueJobOperator,
)
from airflow.sensors.external_task import ExternalTaskSensor
from common import Interval, Layer, Owner

# DAG 기본 설정
default_args = {
    "owner": Owner.MINHYEOK,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# DAG 정의
with DAG(
    dag_id="slv_coin_daily",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=[Layer.SILVER, Interval.DAILY],
) as dag:
    # bronze 레이어 DAG 완료 대기
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="brz_coin_daily",
        external_task_id="fetch_coin_data",  # bronze DAG의 마지막 task
        timeout=600,  # 10분
        mode="reschedule",  # 실패 시 재시도 방식
        poke_interval=60,  # 1분마다 확인
    )

    run_crawler_task = GlueCrawlerOperator(
        task_id="run_crawler_task",
        config={
            "Name": "coin_crawler",
            "Role": "AWSGlueServiceRole-Team3-1",
            "DatabaseName": "team3-db",
            "Targets": {"S3Targets": [{"Path": "s3://team3-1-s3/bronze/coin_data/"}]},
        },
        aws_conn_id="aws_conn_id",
        wait_for_completion=True,
        region_name="ap-northeast-2",
    )

    coin_glue_job = GlueJobOperator(
        task_id="coin_glue_job",
        job_name="slv_coin_daily",  # Glue Job의 이름
        script_location="s3://team3-1-s3/glue_job_scripts/coin_glue_job.py",  # Glue Job의 스크립트가 위치한 S3 경로
        region_name="ap-northeast-2",  # Glue Job이 실행될 AWS 리전
        iam_role_name="AWSGlueServiceRole-Team3-1",  # Glue Job이 사용할 IAM 역할
        num_of_dpus=2,  # Glue Job에 할당할 DPUs 수
        create_job_kwargs={
            "GlueVersion": "3.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        aws_conn_id="aws_conn_id",
    )

    # 태스크 순서 설정
    wait_for_bronze >> run_crawler_task >> coin_glue_job
