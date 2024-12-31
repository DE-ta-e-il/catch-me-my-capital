import json
import tempfile

import pendulum
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from brz_economic_indicators_yearly.constants import StatCode
from common.constants import S3_PARTITION_KEY, AwsConfig, ConnId, Layer
from hooks.bank_of_korea_hook import BankOfKoreaHook


class BankOfKoreaOperator(PythonOperator):
    S3_KEY_TEMPLATE = "{layer}/{stat_name}/{partition_key}={date}/data.json"

    def __init__(self, *args, **kwargs):
        super().__init__(python_callable=self.fetch_statistics, *args, **kwargs)

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

    def fetch_statistics(self, interval, stat_name, **kwargs):
        bank_of_korea_hook = BankOfKoreaHook(conn_id=ConnId.BANK_OF_KOREA)
        date = self.format_date(interval, kwargs["logical_date"])

        stat_data = bank_of_korea_hook.get_statistics(
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

    def format_date(
        self,
        interval,
        date: pendulum.DateTime,
    ):
        if interval == "YEARLY":
            return date.year
