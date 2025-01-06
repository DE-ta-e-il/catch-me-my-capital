import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from psycopg2 import sql
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Spark DataFrame으로 직접 읽기
bronze_df = (
    spark.read.option("multiline", "true")
    .option("recursiveFileLookup", "true")
    .json("s3://team3-1-s3/bronze/msci_index/")
)

# 파일 경로 및 ymd 추가
bronze_df = bronze_df.withColumn("file_path", input_file_name())
bronze_df = bronze_df.withColumn(
    "ymd", regexp_extract("file_path", "ymd=([^/]+)", 1)
).drop("file_path")

# 필요한 컬럼만 선택
silver_df = bronze_df.select("ymd".alias("date"), "close".alias("price"), "index_name")

# create_at, update_at 컬럼 추가
silver_df = silver_df.withColumn("create_at", current_timestamp())
silver_df = silver_df.withColumn("update_at", current_timestamp())

# S3에 저장
silver_df = silver_df.coalesce(1)  # 파티션을 1개로 통합
silver_df.write.mode("overwrite").parquet("s3://team3-1-s3/silver/msci_index")

# Spark DataFrame을 DynamicFrame으로 변환
silver_dynamic_frame = DynamicFrame.fromDF(
    silver_df, glueContext, "silver_dynamic_frame"
)


def get_secret():
    secret_name = "team3-1-redshift-access"  # pragma: allowlist secret
    region_name = "ap-northeast-2"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])
    return secret["username"], secret["password"]


secrets = get_secret()

# Redshift에 저장
WriteToRedshift = glueContext.write_dynamic_frame.from_options(
    frame=silver_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "fact_msci_index",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "DROP TABLE IF EXISTS fact_msci_index; CREATE TABLE fact_msci_index (date DATE, price DECIMAL(18, 5), index_name VARCHAR, create_at TIMESTAMP, update_at TIMESTAMP);",
    },
    transformation_ctx="WriteToRedshift",
)

job.commit()
