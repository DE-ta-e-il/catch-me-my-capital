# NOTE: This was helpful : https://github.com/navin5556/aws-glue-etl-project/blob/main/python_script/MyGlueInsertRedshift.py
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
from pyspark.sql.functions import current_timestamp

#  Glue job validation
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Copy paste this if GICS needs to be silver screened ;P
LoadFromGlueDB_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="krx_codes",
    # The state of the job is distinguished by this 'id', to quote, a "bookmark"
    # NOTE: 'Job bookmark' section  must be enabled for this to work
    # Same as task ids in airflow??
    # https://stackoverflow.com/questions/48300719/what-is-transformation-ctx-used-for-in-aws-glue
    transformation_ctx="LoadFromGlueDB_node1",
)

bronze_df = LoadFromGlueDB_node1.toDF()
bronze_df.createOrReplaceTempView("brz_industry_code")
# Nothing to change...yet...
# Encoding problems may arise ?
silver_df = spark.sql("""
    SELECT
        item_code,
        item_name,
        industry_code,
        market,
        TO_DATE(issue_date, 'yyyy-MM-dd') AS issue_date
    FROM
        brz_industry_code
""")

stamped = silver_df.withColumn("created_at", current_timestamp()).withColumn(
    "updated_at", current_timestamp()
)

# Revert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(stamped, glueContext, "dynamic_frame")

# Write object to S3
# Parquet gets overwritten with new data
WriteToS3_node2 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://team3-1-s3/silver/industry_code/dim_industry_code",
    },
    format="parquet",
    transformation_ctx="WriteToS3_node2",
)


# Secret test
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

# Write to Redshift
WriteToRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.dim_industry_code",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "DROP TABLE IF EXISTS silver.dim_industry_code; CREATE TABLE silver.dim_industry_code (item_code VARCHAR, item_name VARCHAR, industry_code VARCHAR, market VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, issue_date DATE);",
    },
    transformation_ctx="WriteToRedshift_node3",
)

job.commit()
