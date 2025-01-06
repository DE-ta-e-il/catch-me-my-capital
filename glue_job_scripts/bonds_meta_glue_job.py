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

LoadFromGlueDB_govt_bonds_meta_kr = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="govt_bonds_kr",
    transformation_ctx="LoadFromGlueDB_govt_bonds_meta_kr",
)
LoadFromGlueDB_govt_bonds_meta_us = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="govt_bonds_us",
    transformation_ctx="LoadFromGlueDB_govt_bonds_meta_us",
)
LoadFromGlueDB_corp_bonds_meta_kr = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="corp_bonds_kr",
    transformation_ctx="LoadFromGlueDB_corp_bonds_meta_kr",
)
LoadFromGlueDB_corp_bonds_meta_us = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="corp_bonds_us",
    transformation_ctx="LoadFromGlueDB_corp_bonds_meta_us",
)

brz_gk_df = LoadFromGlueDB_govt_bonds_meta_kr.toDF().withColumn("bond_type", "govt")
brz_gu_df = LoadFromGlueDB_govt_bonds_meta_us.toDF().withColumn("bond_type", "corp")
brz_ck_df = LoadFromGlueDB_corp_bonds_meta_kr.toDF().withColumn("bond_type", "govt")
brz_cu_df = LoadFromGlueDB_corp_bonds_meta_us.toDF().withColumn("bond_type", "corp")

concat_df = brz_gk_df.union(brz_gu_df).union(brz_ck_df).union(brz_cu_df)

# NOTE: These operations need be observed (Glue crawler might add underbars or not)
stamped = (
    concat_df.withColumn("created_at", current_timestamp())
    .withColumn("updated_at", current_timestamp())
    .withColumnRenamed(
        "no__of_payments_per_year", "no_of_payments_per_year"
    )  # yap this is precarious
    .withColumnRenamed("floater_", "isfloater")
)
# 'Issuance' is the correct one but i'm sure the source is like this
stamped.drop("issueance", "quotation_type", "special_coupon_type", "payment_frequency")


def table_exists():
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(
        Bucket="team3-1-s3", Prefix="silver/bonds_meta/fact_bonds_meta/"
    )
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
    # Side note: What horrible normalization...
    stamped = (
        existing_df.alias("existing")
        .join(stamped.alias("new"), ["bond_key"], "outer")
        .select(*column_selector)
    )

# Write back to s3
stamped.write.mode("overwrite").parquet(
    "s3://team3-1-s3/silver/bonds_meta/fact_bonds_meta"
)

# Revert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(stamped, glueContext, "dynamic_frame")


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

# Full-refresh the upserted frame to Redshift cuz who needs this bs
WriteToRedshift_bonds = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://team3-1-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev",
        "user": secrets[0],
        "password": secrets[1],
        "dbtable": "silver.fact_bonds_meta",
        "redshiftTmpDir": "s3://team3-1-s3/data/redshift_temp/",
        "preactions": "DROP TABLE IF EXISTS silver.fact_bonds_meta; CREATE TABLE silver.fact_bonds_meta (isin VARCHAR, name VARCHAR, country VARCHAR, issuer VARCHAR, issue_volume BIGINT, currency VARCHAR, issue_price DECIMAL(6, 3), issue_date DATE, coupon DECIMAL(4, 3), denomination BIGINT, payment_type VARCHAR, maturity_date DATE, coupon_payment_date DATE, no_of_payments_per_year DECIMAL(3, 1), coupon_start_date DATE, final_coupon_date DATE, isfloater BOOLEAN, bond_key VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, bond_type VARCHAR);",
    },
    transformation_ctx="WriteToRedshift_bonds",
)

job.commit()
