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
    TICKER_LIST = ["CL=F", "BZ=F", "GC=F"]
    S3_BUCKET = Variable.get("s3_bucket")

    def _generate_s3_key(layer: str, date: str) -> str:
        """데이터를 저장하기 위한 S3 키 값을 생성하는 함수"""

        return f"{layer}/commodities/date={date}/data.csv"

    def _ingest_data_to_s3_landing(
        ticker_list: List[str], execution_date: pendulum.DateTime
    ) -> None:
        """yfinance로부터 데이터를 수집하고, 임시 저장소(S3 landing)에 저장"""

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
        """임시 저장소(S3 landing)에 저장된 데이터에 대한 기본적인 검증"""

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
        """임시 저장소(S3 landing)에 저장된 데이터를 타깃 저장소(S3 bronze)로 이동"""

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
