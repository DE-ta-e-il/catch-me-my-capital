import json
import tempfile

import pendulum
import requests
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from brz_economic_indicators_yearly.constants import IntervalCode, StatCode
from common.constants import S3_PARTITION_KEY, AwsConfig, ConnId, Layer


class BankOfKoreaOperator(PythonOperator):
    BASE_URL = "https://ecos.bok.or.kr/api"
    ENDPOINT = "StatisticSearch"
    S3_KEY_TEMPLATE = "{layer}/{stat_name}/{partition_key}={date}/data.json"

    def __init__(self, *args, **kwargs):
        super().__init__(python_callable=self.fetch_statistics, *args, **kwargs)

    def fetch_statistics(self, interval, stat_name, **kwargs):
        date = self._format_date(interval, kwargs["logical_date"])

        stat_data = self._get_data(
            stat_code=StatCode[stat_name],
            interval=interval,
            date=date,
            batch_size=100,
        )

        s3_key = self.S3_KEY_TEMPLATE.format(
            s3_bucket=Variable.get(AwsConfig.S3_BUCKET_KEY),
            layer=Layer.BRONZE,
            stat_name=stat_name.lower(),
            partition_key=S3_PARTITION_KEY,
            date=kwargs["ds"],
        )

        self._upload_to_s3(stat_data, s3_key)

    def _get_data(self, stat_code, date, interval, batch_size=100):
        api_key = Variable.get("BANK_OF_KOREA_API_KEY")

        all_data = []
        start_index = 1

        while True:
            request_url = f"{self.BASE_URL}/{self.ENDPOINT}/{api_key}/json/kr/{start_index}/{start_index+batch_size-1}/{stat_code}/{IntervalCode[interval]}/{date}/{date}"
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

    def _format_date(
        self,
        interval,
        date: pendulum.DateTime,
    ):
        if interval == "YEARLY":
            return date.year
