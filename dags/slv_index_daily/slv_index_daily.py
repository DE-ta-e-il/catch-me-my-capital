from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
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
    dag_id="slv_index_daily",
    default_args=default_args,
    schedule_interval="0 0 * * 1-5",
    catchup=False,
    tags=[Layer.SILVER, Interval.DAILY.label],
) as dag:
    # bronze 레이어 DAG 완료 대기
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="brz_index_daily",
        external_task_id="fetch_index_data",  # bronze DAG의 마지막 task
        timeout=1800,  # 30분
        mode="reschedule",  # 실패 시 재시도 방식
        poke_interval=60,  # 1분마다 확인
    )

    msci_glue_job = GlueJobOperator(
        task_id="index_glue_job",
        job_name="team3_slv_index_daily",  # Glue Job의 이름
        script_location="s3://team3-1-s3/glue_job_scripts/index_glue_job.py",  # Glue Job의 스크립트가 위치한 S3 경로
        region_name="ap-northeast-2",  # Glue Job이 실행될 AWS 리전
        iam_role_name="AWSGlueServiceRole-Team3-1",  # Glue Job이 사용할 IAM 역할
        num_of_dpus=6,  # Glue Job에 할당할 DPUs 수
        create_job_kwargs={
            "GlueVersion": "5.0",
            "MaxCapacity": 10,
        },
        aws_conn_id="aws_conn_id",
    )

    # 태스크 순서 설정
    wait_for_bronze >> msci_glue_job
