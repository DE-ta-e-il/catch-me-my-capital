# TODO: Redshift auto-column-appending-function (created at etc etc)
# TODO: Redshift connection .env
# TODO: Add crawl exclusion on GICS
# NOTE: This was helpful : https://github.com/navin5556/aws-glue-etl-project/blob/main/python_script/MyGlueInsertRedshift.py
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

LoadFromGlueDB_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db",
    table_name="industry_code",
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
        market
    FROM
        brz_industry_code
""")

# Revert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(silver_df, glueContext, "dynamic_frame")

# Write object to S3
WriteToS3_node2 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://team3-1-s3/silver/industry_code/dim_industry_code",
    },
    format="parquet",
    transformation_ctx="WriteToS3_node2",
)

# Write to Redshift
WriteToRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://team3-1-s3/data/",
        "useConnectionProperties": "true",
        "dbtable": "tunacome.dim_industry_code",
        "connectionName": "redshift_conn_id",
        "preactions": "CREATE TABLE IF NOT EXISTS tunacome.dim_industry_code (item_code VARCHAR, item_name VARCHAR, industry_code VARCHAR, market VARCHAR);",
    },
    transformation_ctx="WriteToRedshift_node3",
)

job.commit()
