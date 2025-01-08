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
    regexp_extract,
    to_date,
)
from pyspark.sql.types import DecimalType, IntegerType

args = getResolvedOptions(sys.argv + ["--JOB_NAME", "default_job_name"], ["JOB_NAME"])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


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
    query = "SELECT 1 FROM pg_tables WHERE tablename = 'fact_coin_data' AND schemaname = 'silver'"

    try:
        result_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
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
        return datetime.strptime("2018-01-01", "%Y-%m-%d").date()

    jdbc_url = "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev"
    query = "(SELECT MAX(date) AS max_date FROM silver.fact_coin_data)"

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
            return datetime.strptime("2018-01-01", "%Y-%m-%d").date()
        return latest_date
    except Exception as e:
        print(f"Error accessing Redshift: {e}")
        return datetime.strptime("2018-01-01", "%Y-%m-%d").date()


# 가장 최근 날짜 가져오기
latest_date = get_latest_date_from_redshift()


# S3에서 특정 날짜 이후의 경로 가져오기
def get_s3_paths_after_date(bucket_name, prefix, start_date):
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator(
        "list_objects_v2"
    )  # S3 버킷의 객체 목록을 가져오는 Paginator 객체 생성
    operation_parameters = {
        "Bucket": bucket_name,
        "Prefix": prefix,
    }  # 버킷 이름 및 경로 prefix 설정
    page_iterator = paginator.paginate(**operation_parameters)  # 버킷 객체 리스트 생성

    paths = []
    for page in page_iterator:
        for obj in page.get("Contents", []):  # 각 페이지 내 객체 목록 가져오기
            key = obj["Key"]  # 객체 키 가져오기
            if "ymd=" in key:
                ymd_str = key.split("ymd=")[1].split("/")[0]  # ymd= 이후 날짜 추출
                ymd_date = datetime.strptime(
                    ymd_str, "%Y-%m-%d"
                ).date()  # 날짜 형식 변환 후 비교
                if (
                    ymd_date > start_date
                ):  # 날짜가 시작 날짜(redshift에 저장된 가장 최근 날짜)보다 크면 경로 추가
                    paths.append(f"s3://{bucket_name}/{key}")
    return list(set(paths))


# S3에서 특정 날짜 이후의 경로 가져오기
bucket_name = "team3-1-s3"
prefix = "bronze/coin_data/"
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

# silver df 생성
silver_df = bronze_df.select(
    to_date(col("ymd"), "yyyy-MM-dd").alias("date"),
    col("close").cast(DecimalType(18, 5)).alias("price"),
    col("volume").cast(DecimalType(18, 5)),
    col("quote_asset_volume").cast(DecimalType(18, 5)),
    col("number_of_trades").cast(IntegerType()),
    col("taker_buy_base_asset_volume").cast(DecimalType(18, 5)),
    col("symbol"),
    current_timestamp().alias("create_at"),
    current_timestamp().alias("update_at"),
)

# # S3에 저장
# silver_df = silver_df.coalesce(1)  # 파티션을 1개로 통합
# silver_df.write.mode("overwrite").parquet("s3://team3-1-s3/silver/coin_data")

# Spark DataFrame을 DynamicFrame으로 변환
silver_dynamic_frame = DynamicFrame.fromDF(
    silver_df, glueContext, "silver_dynamic_frame"
)

# Redshift에 저장
WriteToRedshift = glueContext.write_dynamic_frame.from_options(
    frame=silver_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.fact_coin_data",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": """
            CREATE TABLE IF NOT EXISTS silver.fact_coin_data (
                date DATE,
                price DECIMAL(18, 5),
                volume DECIMAL(18, 5),
                quote_asset_volume DECIMAL(18, 5),
                number_of_trades INT,
                taker_buy_base_asset_volume DECIMAL(18, 5),
                symbol VARCHAR,
                create_at TIMESTAMP,
                update_at TIMESTAMP
            );
        """,
    },
    transformation_ctx="WriteToRedshift",
)

job.commit()
