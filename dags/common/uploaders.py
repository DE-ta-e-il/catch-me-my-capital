import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.constant import S3_BUCKET


def upload_file_to_s3(file_path, key):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.load_file(
        filename=file_path,
        key=key,
        bucket_name=S3_BUCKET,
        replace=True,
    )

    os.remove(file_path)
