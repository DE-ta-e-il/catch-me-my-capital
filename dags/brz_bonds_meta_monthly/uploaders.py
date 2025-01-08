from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_bonds_meta_monthly.constants import ProvidersParam


# Bonds meta data uploader
def upload_bonds_metadata_to_s3(payload: str, key: str):
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    s3.load_string(
        string_data=payload,
        bucket_name=ProvidersParam.S3_BUCKET.value,
        key=key,
        replace=True,
    )
