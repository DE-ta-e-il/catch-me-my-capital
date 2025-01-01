import json
import tempfile
from typing import Any, Dict, List

import pendulum
import requests
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.bank_of_korea_constants import Stat
from common.constants import AwsConfig, ConnId, Interval, Layer


class BankOfKoreaOperator(PythonOperator):
    BASE_URL = "https://ecos.bok.or.kr/api"
    ENDPOINT = "StatisticSearch"
    # NOTE: 같은 지표라도 수집 주기가 여러 가지인 것이 존재해서 S3 키 이름에 주기도 포함한다.
    S3_KEY_TEMPLATE = "{layer}/{interval}_{stat_name}/{partition_key}={date}/data.json"

    def __init__(self, *args, **kwargs):
        super().__init__(python_callable=self.fetch_statistics, *args, **kwargs)

    def fetch_statistics(self, interval: str, stat_name: str, **kwargs) -> None:
        """
        한국은행 API에서 통계 데이터를 수집하고 S3에 업로드하는 작업을 수행합니다.

        Args:
            interval (str): 데이터 수집 주기
            stat_name (str): 수집할 통계 데이터 이름
        """

        formatted_date = self._format_date(kwargs["logical_date"], interval)

        stat_data = self._fetch_statistics_from_api(
            stat_code=Stat[stat_name].code,
            interval=interval,
            date=formatted_date,
            batch_size=100,
        )

        s3_key = self.S3_KEY_TEMPLATE.format(
            s3_bucket=Variable.get(AwsConfig.S3_BUCKET_KEY),
            layer=Layer.BRONZE,
            interval=interval.lower(),
            stat_name=stat_name.lower(),
            partition_key=AwsConfig.S3_PARTITION_KEY,
            date=kwargs["ds"],
        )

        self._upload_data_to_s3(stat_data, s3_key)

    def _fetch_statistics_from_api(
        self, stat_code: str, date: str, interval: str, batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        한국은행 API를 호출하여 지정한 통계 데이터를 수집합니다.

        Args:
            stat_code (str): 조회할 통계 데이터의 코드
            date (str): 데이터 조회 기준 날짜
            interval (str): 데이터 수집 주기
            batch_size (int, optional): 한번의 호출에서 가져올 데이터의 최대 개수. 기본값은 100

        Raises:
            ValueError: API 응답에 "RESULT" 키가 포함된 경우, 조회할 데이터가 없음을 의미
            requests.exceptions.RequestException: API 호출 중 네트워크 오류 또는 HTTP 요청 오류가 발생한 경우

        Returns:
            List[Dict[str, Any]]: 수집된 통계 데이터 결과가 담긴 리스트
        """

        api_key = Variable.get("BANK_OF_KOREA_API_KEY")

        all_data = []
        start_index = 1

        while True:
            request_url = f"{self.BASE_URL}/{self.ENDPOINT}/{api_key}/json/kr/{start_index}/{start_index+batch_size-1}/{stat_code}/{Interval[interval].code}/{date}/{date}"
            response = requests.get(request_url)
            response.raise_for_status()

            data = response.json()

            # NOTE: 조회 기간에 해당하는 데이터가 없으면 "RESULT" 키를 포함하는 응답이 반환된다.
            if "RESULT" in data:
                raise ValueError("No data available for the query.")

            total_count = data[self.ENDPOINT].get("list_total_count", 0)

            if self.ENDPOINT in data and "row" in data[self.ENDPOINT]:
                batch_data = data[self.ENDPOINT]["row"]
                all_data.extend(batch_data)
                print(f"Fetched {len(batch_data)} records.")
            else:
                print("No more data or invalid response.")
                break

            start_index += batch_size
            if start_index > total_count:
                break

        return all_data

    def _upload_data_to_s3(self, data: List[Dict[str, Any]], s3_key: str) -> None:
        """
        데이터를 지정한 S3 경로에 업로드합니다.

        Args:
            data (List[Dict[str, Any]]): 업로드할 데이터가 담긴 리스트
            s3_key (str): 업로드할 S3 키
        """

        s3_hook = S3Hook(aws_conn_id=ConnId.AWS)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", encoding="utf-8"
        ) as temp_file:
            json.dump(data, temp_file, indent=4, ensure_ascii=False)

            temp_file.flush()

            s3_hook.load_file(
                filename=temp_file.name,
                bucket_name=Variable.get(AwsConfig.S3_BUCKET_KEY),
                key=s3_key,
                replace=True,
            )

    def _format_date(self, date: pendulum.DateTime, interval: str) -> str:
        """
        날짜와 수집 주기에 따라 날짜를 포맷팅해서 반환합니다.

        Args:
            date (pendulum.DateTime): 기준 날짜
            interval (str): 데이터 수집 주기

        Returns:
            str: 포맷이 변환된 날짜 문자열
        """

        year, quarter, month, day = date.year, date.quarter, date.month, date.day

        formatted_dates = {
            "DAILY": f"{year}{month:02}{day:02}",
            "MONTHLY": f"{year}{month:02}",
            "QUARTERLY": f"{year}Q{quarter}",
            "YEARLY": f"{year}",
        }

        return formatted_dates[interval]
