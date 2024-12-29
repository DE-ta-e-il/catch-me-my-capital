import logging
import tempfile
from typing import Any, Dict, List

import pandas as pd
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.constants import Layer


class CommoditiesPipeline:
    S3_KEY_TEMPLATE = "{layer}/commodities/date={date}/data.csv"
    REQUIRED_COLUMNS = ["Date", "Ticker", "Close", "High", "Low", "Open", "Volume"]

    def __init__(self, ticker_list: List[str], s3_bucket: str, s3_hook: S3Hook) -> None:
        self.ticker_list = ticker_list
        self.s3_bucket = s3_bucket
        self.s3_hook = s3_hook
        self.logger = logging.getLogger(__name__)

    def extract(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        데이터를 yfinance에서 추출합니다.

        Args:
            start_date (str): 추출할 데이터의 시작일 (YYYY-MM-DD)
            end_date (str): 추출할 데이터의 종료일 (YYYY-MM-DD) (포함되지 않음)

        Returns:
            pd.DataFrame: 추출된 데이터
        """

        self.logger.info(
            f"Fetching data for {self.ticker_list} from {start_date} to {end_date}"
        )
        data = (
            yf.download(self.ticker_list, start=start_date, end=end_date, interval="1d")
            .stack(level=1)
            .reset_index()
        )

        if data.empty:
            raise AirflowSkipException("No data available for the given date range.")

        self.logger.info(f"Fetched {data.shape[0]} rows.")
        return data

    def validate(self, data: pd.DataFrame) -> None:
        """
        데이터 검증을 수행합니다.

        Args:
            data (pd.DataFrame): 검증할 데이터프레임

        Raises:
            ValueError: 필수 컬럼이 없는 경우
        """
        self.logger.info("Start running validation.")

        missing_columns = [
            col for col in self.REQUIRED_COLUMNS if col not in data.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Validation failed: Missing required columns: {', '.join(missing_columns)}"
            )

        self.logger.info("Data validation completed successfully.")

    def load(self, data: pd.DataFrame, s3_key: str) -> None:
        """
        데이터를 S3에 업로드합니다.

        Args:
            data (pd.DataFrame): 업로드할 데이터
            s3_key (str): S3에 저장될 키 이름
        """

        with tempfile.NamedTemporaryFile(mode="w") as temp_file:
            data.to_csv(temp_file.name, index=False)

            self.logger.info(f"Uploading data to s3://{self.s3_bucket}/{s3_key}")
            self.s3_hook.load_file(
                filename=temp_file.name,
                bucket_name=self.s3_bucket,
                key=s3_key,
                replace=True,
            )

        self.logger.info(f"Uploaded data to s3://{self.s3_bucket}/{s3_key}")

    def run(self, **context: Dict[str, Any]) -> None:
        """
        데이터 파이프라인을 실행합니다.

        Args:
            context (Dict[str, Any]): Airflow 실행 컨텍스트
        """
        self.logger.info("Start running pipeline")

        start_date = context["data_interval_start"].strftime("%Y-%m-%d")
        end_date = context["data_interval_end"].strftime("%Y-%m-%d")

        s3_key = self.S3_KEY_TEMPLATE.format(layer=Layer.BRONZE, date=start_date)

        commodities_data = self.extract(start_date, end_date)
        self.validate(commodities_data)
        self.load(commodities_data, s3_key)

        self.logger.info("Pipeline completed successfully.")
