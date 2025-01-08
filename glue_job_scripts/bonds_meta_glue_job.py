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
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://team3-1-s3/bronze/bonds_meta/"],
        "recurse": True,
        "groupFiles": "inPartition",
    },
    format="json",
).toDF()

stamped = (
    df.withColumn("created_at", current_timestamp())
    .withColumn("updated_at", current_timestamp())
    .withColumnRenamed("floater", "isfloater")
)


def table_exists():
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket="team3-1-s3", Prefix="silver/bonds_meta/")
    if "Contents" in resp:
        for obj in resp["Contents"]:
            if obj["Key"].endswith(".parquet"):
                return True
    return False


if table_exists():
    existing_df = spark.read.parquet(
        "s3://team3-1-s3/silver/bonds_meta/fact_bonds_meta"
    )

    column_selector = [
        F.coalesce(F.col("new.updated_at"), F.col("existing.updated_at")).alias(
            "updated_at"
        )
        if col == "updated_at"
        else col
        for col in stamped.columns
    ]
    stamped = (
        existing_df.alias("existing")
        .join(stamped.alias("new"), ["bond_key"], "outer")
        .select(*column_selector)
    )

# Write back to s3
stamped.write.mode("overwrite").parquet("s3://team3-1-s3/silver/bonds_meta")

# Revert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(stamped, glueContext, "dynamic_frame")


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
    return secret["username"], secret["password"], redshift_jdbc["redshift_jdbc_conn"]


secrets = get_secret()

# Full-refresh the upserted frame to Redshift cuz who needs this bs
WriteToRedshift_bonds = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": secrets[2],
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.fact_bonds_meta",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/bonds_meta/",
        "preactions": "DROP TABLE IF EXISTS silver.fact_bonds_meta; CREATE TABLE silver.fact_bonds_meta (isin VARCHAR, name VARCHAR, country VARCHAR, issuer VARCHAR, issue_volume BIGINT, currency VARCHAR, issue_price DECIMAL(6, 3), issue_date DATE, coupon DECIMAL(4, 3), denomination DECIMAL(10, 2), payment_type VARCHAR, maturity_date DATE, coupon_payment_date DATE, no_of_payments_per_year DECIMAL(3, 1), coupon_start_date DATE, final_coupon_date DATE, isfloater BOOLEAN, bond_key VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, bond_type VARCHAR);",
    },
    transformation_ctx="WriteToRedshift_bonds",
)

job.commit()
