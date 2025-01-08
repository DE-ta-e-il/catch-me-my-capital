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
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "current_year"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Secret
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
USER = secrets[0]
PASSWORD = secrets[1]

SCHEMA = "silver"
CALENDAR_TABLE = "dim_calendar"
TEMP_TABLE = "temp_holiday"


source_catalog = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="kr_market_holiday",
    transformation_ctx="source_catalog",
)

holiday_df = (
    source_catalog.toDF()
    .withColumn("holiday_info", F.explode("block1"))
    .select("holiday_info.calnd_dd_dy")
    .filter(F.col("year") == args["current_year"])
)

redshift_df = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": USER,
        "password": PASSWORD,
        "redshiftTmpDir": "s3://team3-1-s3/data/holiday_temp/",
        "dbtable": f"{SCHEMA}.{CALENDAR_TABLE}",
        # "connectionName": 이거 설정
    },
    transformation_ctx="source_redshift",
).toDF()

joined_df = redshift_df.join(
    holiday_df,
    redshift_df["date"] == holiday_df["calnd_dd_dy"],  # JOIN 조건
    "inner",  # INNER JOIN
)


updated_df = (
    joined_df.select(
        *[
            redshift_df[col] for col in redshift_df.columns
        ]  # redshift_df의 모든 컬럼만 선택
    )
    .withColumn("is_market_holiday", F.lit(True))
    .withColumn("updated_at", F.lit(datetime.now()))
)

temp_dyf = DynamicFrame.fromDF(updated_df, glueContext, "updated_df")


glueContext.write_dynamic_frame.from_options(
    frame=temp_dyf,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": USER,
        "password": PASSWORD,
        "redshiftTmpDir": "s3://team3-1-s3/data/holiday_temp/",
        # "useConnectionProperties": "true",
        "dbtable": f"{SCHEMA}.{TEMP_TABLE}",
        # "connectionName": "redshift_dev_db",
        "preactions": f"""
            DROP TABLE IF EXISTS {SCHEMA}.{TEMP_TABLE};
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TEMP_TABLE} (
                    date DATE PRIMARY KEY,
                    year INTEGER NOT NULL,
                    quarter INTEGER NOT NULL,
                    quarter_id VARCHAR(8) NOT NULL,
                    month_num INTEGER NOT NULL,
                    month_id VARCHAR(6) NOT NULL,
                    month_name VARCHAR(3) NOT NULL,
                    day_of_month INTEGER NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    day_name VARCHAR(10) NOT NULL,
                    is_market_holiday BOOLEAN NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
    },
    transformation_ctx="temp_table",
)


glueContext.write_dynamic_frame.from_options(
    frame=temp_dyf,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": USER,
        "password": PASSWORD,
        "redshiftTmpDir": "s3://team3-1-s3/data/holiday_temp/",
        # "useConnectionProperties": "true",
        "dbtable": f"{SCHEMA}.{CALENDAR_TABLE}",
        "preactions": f"""
            DELETE FROM {SCHEMA}.{CALENDAR_TABLE}
            WHERE date IN (
                    SELECT date 
                    FROM {SCHEMA}.{TEMP_TABLE}
                );
            """,
        "postactions": f"DROP TABLE IF EXISTS {SCHEMA}.{TEMP_TABLE};",
    },
    transformation_ctx="update_holiday",
)


job.commit()
