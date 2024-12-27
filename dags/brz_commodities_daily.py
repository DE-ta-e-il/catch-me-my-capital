import logging
from datetime import timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from constants import Interval, Layer, Owner

default_args = {
    "owner": Owner.DAMI,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="brz_commodities_daily",
    description="Daily pipeline to acquire and store market data for commodities.",
    schedule="@daily",
    start_date=pendulum.datetime(2015, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=[Layer.BRONZE, Interval.DAILY, "commodities"],
) as dag:
    # NOTE: 티커 목록(TICKER_LIST)에 대한 설명
    # - "CL=F": 원유(WTI) 선물
    # - "BZ=F": 원유(브렌트유) 선물
    # - "GC=F": 금 선물
    TICKER_LIST = ["CL=F", "BZ=F", "GC=F"]
    S3_BUCKET = Variable.get("s3_bucket")

    def _generate_s3_key(layer: str, date: str) -> str:
        """
        S3에 데이터를 저장하기 위한 키를 생성합니다.

        Args:
            layer (str): S3의 레이어를 나타내는 문자열 (예: "bronze", "silver")
            date (str): 데이터의 날짜를 나타내는 문자열 (형식: YYYY-MM-DD)

        Returns:
            str: S3 키 경로
        """

        return f"{layer}/commodities/date={date}/data.csv"

    def _ingest_data_to_s3_landing(
        ticker_list: List[str], execution_date: pendulum.DateTime
    ) -> None:
        """
        yfinance를 사용하여 가격 데이터를 수집하고, 임시 저장소(S3의 landing 레이어)에 저장합니다.

        Args:
            ticker_list (List[str]): 데이터를 수집할 종목 코드(티커) 리스트)
            execution_date (pendulum.DateTime): 데이터 수집 기준 날짜

        Raises:
            AirflowSkipException: 지정한 날짜 범위에 데이터가 없는 경우 발생

        Returns:
            None
        """

        import tempfile
        from os import path

        import yfinance as yf
        from airflow.exceptions import AirflowSkipException

        start_date = execution_date.strftime("%Y-%m-%d")
        end_date = (execution_date + timedelta(days=1)).strftime("%Y-%m-%d")
        s3_key_landing = _generate_s3_key(Layer.LANDING, start_date)

        logging.info(f"Fetching data for {ticker_list} from {start_date} to {end_date}")

        data = (
            yf.download(ticker_list, start=start_date, end=end_date, interval="1d")
            .stack(level=1)
            .reset_index()
        )

        logging.info(f"Fetched {data.shape[0]} rows")

        if data.empty:
            raise AirflowSkipException("Data not available for processing.")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = path.join(temp_dir, "commodities.csv")
            data.to_csv(temp_path, index=False)

            logging.info(f"Writing results to s3://{S3_BUCKET}/{s3_key_landing}")

            s3_hook = S3Hook(aws_conn_id="aws_conn_id")
            s3_hook.load_file(
                temp_path,
                bucket_name=S3_BUCKET,
                key=s3_key_landing,
                replace=True,
            )

            logging.info(f"Uploaded to s3://{S3_BUCKET}/{s3_key_landing}")

        return s3_key_landing

    def _validate_data(ti: TaskInstance) -> None:
        """
        임시 저장소에 저장된 데이터에 대한 기초적인 검증을 수행합니다.

        Args:
            ti (TaskInstance): Airflow의 TaskInstance 객체. XCom에서 S3 키를 가져오기 위해 사용합니다.

        Raises:
            FileNotFoundError: 임시 저장소에 검증을 수행하려는 파일이 없는 경우 발생
            ValueError: 파일에 데이터가 없거나, 필수 컬럼이 누락된 경우 발생

        Returns:
            None
        """

        from io import StringIO

        import pandas as pd

        s3_key_landing = ti.xcom_pull(task_ids="ingest_data_to_s3_landing")
        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        s3_obj = s3_hook.get_key(s3_key_landing, S3_BUCKET)

        if not s3_obj:
            raise FileNotFoundError(
                f"{s3_key_landing} does not exist on s3://{S3_BUCKET}"
            )

        csv_content = s3_obj.get()["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))

        if df.shape[0] < 1:
            raise ValueError(
                f"No rows found in file: s3://{S3_BUCKET}/{s3_key_landing}"
            )

        required_columns = ["Date", "Ticker", "Close", "High", "Low", "Open", "Volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        logging.info(f"Passed validation: shape={df.shape}")

    def _upload_data_to_s3_bronze(
        ti: TaskInstance, execution_date: pendulum.DateTime
    ) -> None:
        """
        임시 저장소에 저장된 데이터를 실제 저장소(S3의 bronze 레이어)로 이동합니다.

        Args:
            ti (TaskInstance): Airflow의 TaskInstance 객체. XCom에서 S3 키를 가져오기 위해 사용합니다.
            execution_date (pendulum.DateTime): 데이터 수집 기준 날짜

        Raises:
            botocore.exceptions.ClientError: S3에서 파일 복사 중 실패한 경우 발생

        Returns:
            None
        """

        s3_hook = S3Hook(aws_conn_id="aws_conn_id")
        s3_key_landing = ti.xcom_pull(task_ids="ingest_data_to_s3_landing")
        s3_key_bronze = _generate_s3_key(
            Layer.BRONZE, execution_date.strftime("%Y-%m-%d")
        )

        s3_hook.copy_object(
            source_bucket_name=S3_BUCKET,
            source_bucket_key=s3_key_landing,
            dest_bucket_name=S3_BUCKET,
            dest_bucket_key=s3_key_bronze,
        )
        logging.info(f"Copied from {s3_key_landing} to {s3_key_bronze}")

        logging.info(f"Deleting original key: {s3_key_landing}")
        s3_hook.delete_objects(bucket=S3_BUCKET, keys=[s3_key_landing])

    ingest_data_to_s3_landing = PythonOperator(
        task_id="ingest_data_to_s3_landing",
        python_callable=_ingest_data_to_s3_landing,
        op_kwargs={"ticker_list": TICKER_LIST},
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    upload_data_to_s3_bronze = PythonOperator(
        task_id="upload_data_to_s3_bronze",
        python_callable=_upload_data_to_s3_bronze,
    )

    ingest_data_to_s3_landing >> validate_data >> upload_data_to_s3_bronze
