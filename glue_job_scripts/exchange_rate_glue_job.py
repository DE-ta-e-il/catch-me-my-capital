import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    regexp_extract,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
LoadFromS3 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://team3-1-s3/bronze/exchange_rate/"],
        "recurse": True,
    },
    transformation_ctx="LoadFromS3",
)

# 파일 경로 및 ymd 추가
bronze_df = LoadFromS3.toDF()
bronze_df = bronze_df.withColumn("file_path", input_file_name())
bronze_df = bronze_df.withColumn(
    "ymd", regexp_extract("file_path", "ymd=([^/]+)", 1)
).drop("file_path")

# silver df 생성 (기준 통화 별로 나누기)
silver_krw_df = (
    bronze_df.select(
        col("ymd").alias("date"),
        col("AUDKRW=X").alias("exchange_rate"),
    )
    .withColumn("currency", lit("AUD"))
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("EURKRW=X").alias("exchange_rate"),
        ).withColumn("currency", lit("EUR"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("GBPKRW=X").alias("exchange_rate"),
        ).withColumn("currency", lit("GBP"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("INRKRW=X").alias("exchange_rate"),
        ).withColumn("currency", lit("INR"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("JPYKRW=X").alias("exchange_rate"),
        ).withColumn("currency", lit("JPY"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("USDKRW=X").alias("exchange_rate"),
        ).withColumn("currency", lit("USD"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("ZARKRW=X").alias("exchange_rate"),
        ).withColumn("currency", lit("ZAR"))
    )
)

silver_usd_df = (
    bronze_df.select(
        col("ymd").alias("date"),
        col("AUDUSD=X").alias("exchange_rate"),
    )
    .withColumn("currency", lit("AUD"))
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("EURUSD=X").alias("exchange_rate"),
        ).withColumn("currency", lit("EUR"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("CNYUSD=X").alias("exchange_rate"),
        ).withColumn("currency", lit("CNY"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("GBPUSD=X").alias("exchange_rate"),
        ).withColumn("currency", lit("GBP"))
    )
    .union(
        bronze_df.select(
            col("ymd").alias("date"),
            col("JPYUSD=X").alias("exchange_rate"),
        ).withColumn("currency", lit("JPY"))
    )
)

# create_at, update_at 컬럼 추가
silver_krw_df = silver_krw_df.withColumn("create_at", current_timestamp())
silver_krw_df = silver_krw_df.withColumn("update_at", current_timestamp())
silver_usd_df = silver_usd_df.withColumn("create_at", current_timestamp())
silver_usd_df = silver_usd_df.withColumn("update_at", current_timestamp())

# S3에 저장
silver_krw_df = silver_krw_df.coalesce(1)  # 파티션을 1개로 통합
silver_krw_df.write.mode("overwrite").parquet(
    "s3://team3-1-s3/silver/exchange_rate/krw"
)
silver_usd_df = silver_usd_df.coalesce(1)  # 파티션을 1개로 통합
silver_usd_df.write.mode("overwrite").parquet(
    "s3://team3-1-s3/silver/exchange_rate/usd"
)

# Spark DataFrame을 DynamicFrame으로 변환
silver_krw_dynamic_frame = DynamicFrame.fromDF(
    silver_krw_df, glueContext, "silver_dynamic_frame"
)
silver_usd_dynamic_frame = DynamicFrame.fromDF(
    silver_usd_df, glueContext, "silver_dynamic_frame"
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
    frame=silver_krw_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "fact_exchange_rate_krw",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "DROP TABLE IF EXISTS fact_exchange_rate_krw; CREATE TABLE fact_exchange_rate_krw (date DATE, currency VARCHAR, exchange_rate DECIMAL(18, 5), create_at TIMESTAMP, update_at TIMESTAMP);",
    },
    transformation_ctx="WriteToRedshift",
)

WriteToRedshift2 = glueContext.write_dynamic_frame.from_options(
    frame=silver_usd_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "fact_exchange_rate_usd",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "DROP TABLE IF EXISTS fact_exchange_rate_usd; CREATE TABLE fact_exchange_rate_usd (date DATE, currency VARCHAR, exchange_rate DECIMAL(18, 5), create_at TIMESTAMP, update_at TIMESTAMP);",
    },
    transformation_ctx="WriteToRedshift2",
)

job.commit()
