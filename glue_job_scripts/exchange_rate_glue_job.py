import json
import sys
from datetime import datetime

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
    to_date,
)
from pyspark.sql.types import DateType, DecimalType, StringType, TimestampType

args = getResolvedOptions(sys.argv + ["--JOB_NAME", "default_job_name"], ["JOB_NAME"])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Secrets Manager에서 Redshift 자격 증명 가져오기
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


# Redshift에서 테이블 존재 여부 확인
def check_table_exists():
    jdbc_url = "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev"
    query = "SELECT 1 FROM pg_tables WHERE tablename = 'fact_exchange_rate_krw' AND schemaname = 'silver'"

    try:
        result_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)  # 'query' 옵션 사용
            .option("user", secrets[0])
            .option("password", secrets[1])
            .load()
        )
        return result_df.count() > 0
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


# Redshift에서 가장 최근 날짜 가져오기
def get_latest_date_from_redshift():
    if not check_table_exists():
        print("Table does not exist, using default date.")
        return datetime.strptime("2015-01-01", "%Y-%m-%d").date()

    jdbc_url = "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev"
    query = "(SELECT MAX(date) AS max_date FROM silver.fact_exchange_rate_krw)"

    try:
        redshift_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", secrets[0])
            .option("password", secrets[1])
            .load()
        )

        latest_date_row = redshift_df.collect()[0]
        latest_date = latest_date_row["max_date"]
        if latest_date is None:
            print("No data found, using default date.")
            return datetime.strptime("2015-01-01", "%Y-%m-%d").date()
        return latest_date
    except Exception as e:
        print(f"Error accessing Redshift: {e}")
        return datetime.strptime("2015-01-01", "%Y-%m-%d").date()


# S3에서 특정 날짜 이후의 경로 가져오기
def get_s3_paths_after_date(bucket_name, prefix, start_date):
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket_name, "Prefix": prefix}
    page_iterator = paginator.paginate(**operation_parameters)

    paths = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "ymd=" in key:
                ymd_str = key.split("ymd=")[1].split("/")[0]
                ymd_date = datetime.strptime(ymd_str, "%Y-%m-%d").date()
                if ymd_date > start_date:
                    paths.append(f"s3://{bucket_name}/{key}")
    return list(set(paths))


# 가장 최근 날짜 가져오기
latest_date = get_latest_date_from_redshift()

# S3에서 특정 날짜 이후의 경로 가져오기
bucket_name = "team3-1-s3"
prefix = "bronze/exchange_rate/"
s3_paths = get_s3_paths_after_date(bucket_name, prefix, latest_date)

# S3 경로가 비어 있을 경우 Job 종료
if not s3_paths:
    print("No new data to process. Exiting job.")
    sys.exit(0)

# S3에서 데이터 로드
LoadFromS3 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": s3_paths,
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
        to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
        col("AUDKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
    )
    .withColumn("currency", lit("AUD").cast(StringType()))
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("EURKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("EUR").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("GBPKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("GBP").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("INRKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("INR").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("JPYKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("JPY").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("USDKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("USD").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("ZARKRW=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("ZAR").cast(StringType()))
    )
)

silver_usd_df = (
    bronze_df.select(
        to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
        col("AUDUSD=X").alias("exchange_rate").cast(DecimalType(18, 5)),
    )
    .withColumn("currency", lit("AUD").cast(StringType()))
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("EURUSD=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("EUR").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("CNYUSD=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("CNY").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("GBPUSD=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("GBP").cast(StringType()))
    )
    .union(
        bronze_df.select(
            to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
            col("JPYUSD=X").alias("exchange_rate").cast(DecimalType(18, 5)),
        ).withColumn("currency", lit("JPY").cast(StringType()))
    )
)

# create_at, update_at 컬럼 추가
silver_krw_df = silver_krw_df.withColumn("create_at", current_timestamp())
silver_krw_df = silver_krw_df.withColumn("update_at", current_timestamp())
silver_usd_df = silver_usd_df.withColumn("create_at", current_timestamp())
silver_usd_df = silver_usd_df.withColumn("update_at", current_timestamp())

# # S3에 저장
# silver_krw_df = silver_krw_df.coalesce(1)  # 파티션을 1개로 통합
# silver_krw_df.write.mode("overwrite").parquet(
#     "s3://team3-1-s3/silver/exchange_rate/krw"
# )
# silver_usd_df = silver_usd_df.coalesce(1)  # 파티션을 1개로 통합
# silver_usd_df.write.mode("overwrite").parquet(
#     "s3://team3-1-s3/silver/exchange_rate/usd"
# )

# Spark DataFrame을 DynamicFrame으로 변환
silver_krw_dynamic_frame = DynamicFrame.fromDF(
    silver_krw_df, glueContext, "silver_dynamic_frame"
)
silver_usd_dynamic_frame = DynamicFrame.fromDF(
    silver_usd_df, glueContext, "silver_dynamic_frame"
)

# Redshift에 저장
WriteToRedshift = glueContext.write_dynamic_frame.from_options(
    frame=silver_krw_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.fact_exchange_rate_krw",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "CREATE TABLE IF NOT EXISTS silver.fact_exchange_rate_krw (date DATE, currency VARCHAR, exchange_rate DECIMAL(18, 5), create_at TIMESTAMP, update_at TIMESTAMP);",
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
        "dbtable": "silver.fact_exchange_rate_usd",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "CREATE TABLE IF NOT EXISTS silver.fact_exchange_rate_usd (date DATE, currency VARCHAR, exchange_rate DECIMAL(18, 5), create_at TIMESTAMP, update_at TIMESTAMP);",
    },
    transformation_ctx="WriteToRedshift2",
)

job.commit()
