import json
import tempfile

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

    def fetch_statistics(self, interval, stat_name, **kwargs):
        formatted_date = self._format_date(kwargs["logical_date"], interval)

        stat_data = self._get_data(
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

        self._upload_to_s3(stat_data, s3_key)

    def _get_data(self, stat_code, date, interval, batch_size=100):
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

    def _upload_to_s3(self, data, s3_key):
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

    def _format_date(self, date: pendulum.DateTime, interval: str):
        year, quarter, month, day = date.year, date.quarter, date.month, date.day

        formatted_dates = {
            "DAILY": f"{year}{month:02}{day:02}",
            "MONTHLY": f"{year}{month:02}",
            "QUARTERLY": f"{year}Q{quarter}",
            "YEARLY": f"{year}",
        }

        return formatted_dates[interval]
