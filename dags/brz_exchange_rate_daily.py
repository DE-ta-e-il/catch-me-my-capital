import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

# 통화 리스트
CURRENCY_PAIRS = [
    "USDKRW=X",  # 미국 달러/한국 원
    "EURKRW=X",  # 유럽 연합 유로/한국 원
    "GBPKRW=X",  # 영국 파운드/한국 원
    "AUDKRW=X",  # 호주 달러/한국 원
    "JPYKRW=X",  # 일본 엔/한국 원
    "INRKRW=X",  # 인도 루피/한국 원
    "ZARKRW=X",  # 남아프리카 공화국 랜드/한국 원
    "EURUSD=X",  # 유럽 연합 유로/미국 달러
    "AUDUSD=X",  # 호주 달러/미국 달러
    "JPYUSD=X",  # 일본 엔/미국 달러
    "CNYUSD=X",  # 중국 위안/미국 달러
    "GBPUSD=X",  # 영국 파운드/미국 달러
]


def fetch_and_upload_exchange_rates(execution_date: datetime, **kwargs):
    """
    환율 데이터 수집 함수
    """
    # 주말일 경우 환율 데이터 미제공(금요일데이터로 동결됨), task 스킵
    if execution_date.weekday() >= 5:  # 5: Saturday, 6: Sunday
        raise AirflowSkipException("Execution date is a weekend. Skipping task.")

    # 데이터 수집
    data = yf.download(
        CURRENCY_PAIRS,
        period="1d",
        start=execution_date,
        end=execution_date + timedelta(days=1),
    )

    # 종가 데이터 추출
    if isinstance(data.columns, pd.MultiIndex):
        close_data = data["Close"]
    else:
        close_data = data[["Close"]]

    # 날짜를 열로 추가
    close_data = close_data.reset_index()
    # Date를 파티션 키로 사용할 경우 충돌을 피하기 위해 칼럼 이름 변경
    close_data.rename(columns={"Date": "RecordDate"}, inplace=True)

    # 임시 파일로 Parquet 저장
    temp_file_path = f"/tmp/exchange_rates_{execution_date.strftime('%Y%m%d')}.parquet"
    close_data.to_parquet(temp_file_path, index=False)

    # S3에 업로드
    s3_upload_task = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=temp_file_path,
        dest_bucket="team3-1-s3",
        dest_key=f"bronze/exchange_rate/date={execution_date.strftime('%Y-%m-%d')}/{execution_date.strftime('%Y-%m-%d')}_exchange_rates.parquet",
        replace=True,
        aws_conn_id="aws_conn_id",
    )

    s3_upload_task.execute(context=kwargs)

    # 임시 파일 삭제
    os.remove(temp_file_path)


default_args = {
    "owner": "JeongMinHyeok",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="brz_exchange_rate_daily",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=["bronze", "daily"],
) as dag:
    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_exchange_rates",
        python_callable=fetch_and_upload_exchange_rates,
        provide_context=True,
    )

    fetch_and_upload_task
