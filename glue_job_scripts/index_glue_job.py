import json
import logging
import os
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
    regexp_replace,
    to_date,
)
from pyspark.sql.types import DateType, DecimalType, StringType

logging.basicConfig(level=logging.INFO)

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
        redshift_jdbc = client.get_secret_value(SecretId="redshift-jdbc-conn")
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])
    jdbc = json.loads(redshift_jdbc["SecretString"])
    return secret["username"], secret["password"], jdbc["redshift_jdbc_conn"]


secrets = get_secret()


# Redshift에서 테이블 존재 여부 확인
def check_table_exists():
    jdbc_url = secrets[2]
    query = "SELECT 1 FROM pg_tables WHERE tablename = 'fact_index_data' AND schemaname = 'silver'"

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
        logging.error(f"테이블 존재 여부 확인 오류: {e}")
        return False


# Redshift에서 가장 최근 날짜 가져오기
def get_latest_date_from_redshift():
    if not check_table_exists():
        logging.info("테이블이 존재하지 않음. 기본 날짜 사용")
        return datetime.strptime("2015-01-01", "%Y-%m-%d").date()

    jdbc_url = secrets[2]
    query = "(SELECT MAX(date) AS max_date FROM silver.fact_index_data)"

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
            logging.info("데이터가 없음. 기본 날짜 사용")
            return datetime.strptime("2015-01-01", "%Y-%m-%d").date()
        return latest_date
    except Exception as e:
        logging.error(f"Redshift 접근 오류: {e}")
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
prefix = "bronze/index_data/"
s3_paths = get_s3_paths_after_date(bucket_name, prefix, latest_date)

# S3 경로가 비어 있을 경우 Job 종료
if not s3_paths:
    logging.info("새로운 데이터가 없음. 작업 종료")
    os._exit(0)

# S3에서 데이터 로드
bronze_df = (
    spark.read.option("multiline", "true")
    .option("recursiveFileLookup", "true")
    .json(s3_paths)
)

# 파일 경로 및 ymd 추가
bronze_df = bronze_df.withColumn("file_path", input_file_name())
bronze_df = bronze_df.withColumn(
    "ymd", regexp_extract("file_path", "ymd=([^/]+)", 1)
).drop("file_path")


# silver df 생성
silver_df = bronze_df.select(
    to_date(col("ymd"), "yyyy-MM-dd").alias("date").cast(DateType()),
    regexp_replace(col("last_close"), ",", "").cast(DecimalType(10, 2)).alias("price"),
    col("change_precent").alias("change_percent").cast(DecimalType(10, 2)),
    col("index_name").cast(StringType()),
)

# create_at, update_at 컬럼 추가
silver_df = silver_df.withColumn("create_at", current_timestamp())
silver_df = silver_df.withColumn("update_at", current_timestamp())


# Spark DataFrame을 DynamicFrame으로 변환
silver_dynamic_frame = DynamicFrame.fromDF(
    silver_df, glueContext, "silver_dynamic_frame"
)

# Redshift에 저장
WriteToRedshift = glueContext.write_dynamic_frame.from_options(
    frame=silver_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": secrets[2],
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.fact_index_data",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "CREATE TABLE IF NOT EXISTS silver.fact_index_data (date DATE, price DECIMAL(10, 2), change_percent DECIMAL(10, 2), index_name VARCHAR, create_at TIMESTAMP, update_at TIMESTAMP);",
    },
    transformation_ctx="WriteToRedshift",
)

job.commit()
