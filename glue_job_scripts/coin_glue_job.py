import sys

from awsglue.context import GlueContext
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

# Glue catalog에서 데이터 로드
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="team3-db", table_name="coin_data", transformation_ctx="datasource0"
)

# Glue DynamicFrame을 Spark DataFrame으로 변환
df = datasource0.toDF()

# Select the required columns and rename 'Close' to 'Price'
selected_df = df.select(
    col("Close").alias("Price"),
    col("Quote_asset_volume"),
    col("Number_of_trades"),
    col("Symbol"),
)

# Spark DataFrame을 Glue DynamicFrame으로 변환
dynamic_frame = DynamicFrame.fromDF(selected_df, glueContext, "dynamic_frame")

# S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://team3-1-s3/silver/coin_data/fact_coin_data.parquet",
        "partitionKeys": ["Date"],
    },
    format="parquet",
)

# Commit the job
job.commit()
