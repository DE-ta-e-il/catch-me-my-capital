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
        "paths": ["s3://team3-1-s3/bronze/bonds/"],
        "recurse": True,
        "groupFiles": "inPartition",
    },
    format="json",
).toDF()

final_df = (
    df.withColumn("created_at", current_timestamp())
    .withColumn("updated_at", current_timestamp())
    .withColumnRenamed("Close", "yield")
    .withColumnRenamed("Volume", "volume")
    .withColumnRenamed("Date", "date")
    .drop("Open", "High", "Low", "Estimate")
)


def table_exists():
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket="team3-1-s3", Prefix="silver/bonds/")
    if "Contents" in resp:
        for obj in resp["Contents"]:
            if obj["Key"].endswith(".parquet"):
                return True
    return False


if table_exists():
    existing_df = spark.read.parquet(
        "s3://team3-1-s3/silver/bonds", {"mergeSchema": True}
    )

    # Upsert
    final_df = (
        existing_df.alias("existing")
        .join(final_df.alias("new"), ["date", "bond_key"], "outer")
        .select(
            F.coalesce(F.col("new.yield"), F.col("existing.yield")).alias("yield"),
            F.coalesce(F.col("new.volume"), F.col("existing.volume")).alias("volume"),
            F.col("new.date").alias("date"),
            F.col("new.bond_key").alias("bond_key"),
            F.coalesce(F.col("new.matures_in"), F.col("existing.matures_in")).alias(
                "matures_in"
            ),
            F.col("new.bond_type").alias("bond_type"),
        )
    )

# Write back to s3
final_df.write.mode("overwrite").parquet("s3://team3-1-s3/silver/bonds/")

# Revert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "dynamic_frame")


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

# Full-refresh the upserted frame to Redshift cuz who needs this bs
WriteToRedshift_bonds = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": secrets[2],
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.fact_bonds",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/bonds/",
        "preactions": "DROP TABLE IF EXISTS silver.fact_bonds; CREATE TABLE silver.fact_bonds (yield DECIMAL(6, 3), volume DECIMAL(12, 2), date TIMESTAMP, matures_in BIGINT, created_at TIMESTAMP, updated_at TIMESTAMP, bond_key VARCHAR, bond_type VARCHAR);",
    },
    transformation_ctx="WriteToRedshift_bonds",
)

job.commit()
