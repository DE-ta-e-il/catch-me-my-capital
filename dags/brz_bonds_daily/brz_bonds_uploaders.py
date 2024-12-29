import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_bonds_daily.brz_bonds_constants import S3_BUCKET


# Bonds uploader: this one is not a task.
def upload_bonds_to_s3(date, target, content):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=json.dumps(content),
        bucket_name=S3_BUCKET,
        key=f"bronze/corp_bonds_kr/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
        replace=True,
    )
