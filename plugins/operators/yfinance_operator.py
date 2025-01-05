import tempfile
from typing import List

import pandas as pd
import pendulum
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.constants import AwsConfig, ConnId, Layer


class YFinanceOperator(PythonOperator):
    S3_KEY_TEMPLATE = "{layer}/{category}/{partition_key}={date}/data.csv"

    def __init__(self, category: str, tickers: List[str], *args, **kwargs):
        super().__init__(python_callable=self.fetch_price_data, *args, **kwargs)
        self.category = category
        self.tickers = tickers

    def fetch_price_data(
        self,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
    ) -> None:
        """
        yfinance에서 데이터를 가져와 S3에 업로드하는 작업을 수행합니다.

        Args:
            data_interval_start (pendulum.DateTime): 데이터 수집 시작 시점
            data_interval_end (pendulum.DateTime): 데이터 수집 종료 시점 (포함되지 않음)
        """

        start_date = data_interval_start.strftime("%Y-%m-%d")
        end_date = data_interval_end.strftime("%Y-%m-%d")

        data = self._fetch_price_data_from_yfinance(start_date, end_date)

        s3_key = self.S3_KEY_TEMPLATE.format(
            layer=Layer.BRONZE,
            category=self.category,
            partition_key=AwsConfig.S3_PARTITION_KEY,
            date=start_date,
        )
        self._upload_data_to_s3(data, s3_key)

    def _fetch_price_data_from_yfinance(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        yfinance에서 데이터를 가져옵니다.

        Args:
            start_date (str): 데이터 수집 시작 날짜
            end_date (str): 데이터 수집 종료 날짜 (포함되지 않음)

        Returns:
            pd.DataFrame: 수집된 데이터
        """

        self.log.info(f"Fetching: category: {self.category}, tickers: {self.tickers}")
        self.log.info(f"Date range: {start_date} to {end_date}")

        data = (
            # NOTE: yfinance에서는 다양한 시간 간격으로 집계된 데이터를 제공하지만,
            # 여기서는 일 단위 데이터(interval="1d")만 사용합니다.
            yf.download(
                tickers=self.tickers,
                start=start_date,
                end=end_date,
                interval="1d",
            )
            .stack(level=1, future_stack=True)
            .reset_index()
            .rename_axis(None, axis=1)
        )

        if data.empty:
            raise AirflowSkipException("No data available for the given date range.")

        fetched_tickers = set(data[data["Close"].notna()]["Ticker"])
        missing_tickers = set(self.tickers) - fetched_tickers

        # TODO: 향후 휴장일 정보(뉴욕상업거래소, ICE 등)를 추가 수집하여
        # 누락된 데이터가 거래소 휴장일 때문인지, 데이터 소스 문제인지 검증하는 로직을 구현해야 합니다.
        if missing_tickers:
            self.log.warning(
                f"Data mismatch detected: Tried to fetch {len(self.tickers)} tickers, "
                f"but fetched {len(fetched_tickers)} rows. Missing tickers: {missing_tickers}. "
            )
        else:
            self.log.info(f"Fetched: {data.shape[0]} rows, {data.shape[1]} columns")
            self.log.info(f"Sample data:\n{data.head()}")

        return data

    def _upload_data_to_s3(self, data: pd.DataFrame, s3_key: str) -> None:
        """
        데이터를 S3에 업로드합니다.

        Args:
            data (pd.DataFrame): 업로드할 데이터
            s3_key (str): 업로드할 S3 키
        """
        self.log.info(f"Start uploading data to S3: {s3_key}")
        s3_hook = S3Hook(aws_conn_id=ConnId.AWS)

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as temp_file:
            data.to_csv(temp_file.name, index=False)

            s3_hook.load_file(
                filename=temp_file.name,
                bucket_name=Variable.get(AwsConfig.S3_BUCKET_KEY),
                key=s3_key,
                replace=True,
            )

        self.log.info(f"Successfully uploaded data to S3: {s3_key}")
