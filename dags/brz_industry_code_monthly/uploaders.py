import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from constants import S3_BUCKET


# Industry code uploader
def upload_codes_to_s3(target, **ctxt):
    with open(f"/tmp/{target}_industry_codes.json", "r") as file:
        data = json.load(file)
        s3 = S3Hook(aws_conn_id="aws_conn_id")
        s3.load_string(
            string_data=json.dumps(data),
            bucket_name=S3_BUCKET,
            key=f"bronze/industry_code/date={ctxt['ds']}/{target}_codes_{ctxt['ds']}.json",
            replace=True,
        )


# Bonds uploader: this one is not a task.
def upload_bonds_to_s3(date, target, content):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=json.dumps(content),
        bucket_name=S3_BUCKET,
        key=f"bronze/corp_bonds_kr/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
        replace=True,
    )


# Bonds meta data uploader
def upload_bonds_metadata_to_s3(date, category, target, content):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=json.dumps(content),
        bucket_name=S3_BUCKET,
        key=f"bronze/{category}/kind={target}/date={date}/{category}_{target}_meta_{date[:7]}.json",
        replace=True,
    )
