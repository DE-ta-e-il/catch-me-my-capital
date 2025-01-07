import json

from airflow.models import Variable


class AwsConfig:
    aws_config = json.loads(Variable.get("aws_config"))

    REGION_NAME = aws_config["region_name"]

    REDSHIFT_CLUSTER_ID = aws_config["redshift_cluster_id"]
    REDSHIFT_DB_NAME = aws_config["redshift_db_name"]
    REDSHIFT_DB_USER = aws_config["redshift_db_user"]
    REDSHIFT_URL = aws_config["redshift_url"]

    S3_BUCKET_NAME = aws_config["s3_bucket_name"]
    S3_PARTITION_KEY = "ymd"

    GLUE_IAM_ROLE_NAME = aws_config["glue_iam_role_name"]
    GLUE_JOB_NAME_TEMPLATE = "team3_{feature}_glue_job"
    GLUE_JOB_DEFAULT_KWARGS = {
        "GlueVersion": "5.0",
        "WorkerType": "G.2X",
        "NumberOfWorkers": 5,
    }
