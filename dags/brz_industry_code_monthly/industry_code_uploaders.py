import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_industry_code_monthly.industry_code_constants import S3_BUCKET


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
