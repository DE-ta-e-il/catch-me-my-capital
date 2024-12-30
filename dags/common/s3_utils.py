from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_string_to_s3(s, s3_key):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=s,
        key=s3_key,
        bucket_name=Variable.get("s3_bucket"),
        replace=True,
    )
