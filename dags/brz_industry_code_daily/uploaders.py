import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_industry_code_daily.constants import S3_BUCKET


# Industry code uploader
def upload_codes_to_s3(payload, key):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=json.dumps(payload),
        bucket_name=S3_BUCKET,
        key=key,
        replace=True,
    )
