import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_bonds_meta_monthly.brz_bonds_meta_constants import S3_BUCKET


# Bonds meta data uploader
def upload_bonds_metadata_to_s3(date, category, target, content):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=json.dumps(content),
        bucket_name=S3_BUCKET,
        key=f"bronze/{category}/kind={target}/date={date}/{category}_{target}_meta_{date[:7]}.json",
        replace=True,
    )
