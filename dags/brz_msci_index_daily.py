import calendar
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)


def fetch_and_upload_indices_data(execution_date: datetime, **kwargs):
    """
    MSCI World Index, MSCI Emerging Market Index 데이터 수집 함수
    """
    if execution_date.weekday() in [0, 6]:  # 1일 전 데이터 수집하므로
        raise AirflowSkipException("Execution date is a weekend. Skipping task.")

    month = calendar.month_abbr[execution_date.month]
    day = (execution_date - timedelta(days=1)).strftime("%d")
    year = execution_date.year

    urls = {
        "MSCI_World": os.getenv("MSCI_WORLD_URL").format(
            month=month, day=day, year=year
        ),
        "MSCI_Emerging": os.getenv("MSCI_EMERGING_URL").format(
            month=month, day=day, year=year
        ),
    }

    data_frames = []
    for index_name, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            df["Index"] = index_name
            data_frames.append(df)
        else:
            raise Exception(
                f"Failed to fetch data for {index_name}: {response.status_code}"
            )

    combined_data = pd.concat(data_frames, ignore_index=True)
    # Date를 파티션 키로 사용할 경우 충돌을 피하기 위해 칼럼 이름 변경
    combined_data.rename(columns={"Date": "RecordDate"}, inplace=True)

    # 임시 파일로 Parquet 저장
    temp_file_path = f"/tmp/indices_data_{execution_date.strftime('%Y%m%d')}.parquet"
    combined_data.to_parquet(temp_file_path, index=False)

    # S3에 업로드
    s3_upload_task = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=temp_file_path,
        dest_bucket="team3-1-s3",
        dest_key=f"bronze/msci_index/date={execution_date.strftime('%Y-%m-%d')}/{execution_date.strftime('%Y-%m-%d')}_msci_index.parquet",
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
    dag_id="brz_msci_index_daily",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=["bronze", "daily"],
) as dag:
    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_indices_data",
        python_callable=fetch_and_upload_indices_data,
        provide_context=True,
    )

    fetch_and_upload_task
