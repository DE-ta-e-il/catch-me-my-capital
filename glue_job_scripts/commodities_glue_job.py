import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

glue_ctx = GlueContext(SparkContext.getOrCreate())
logger = glue_ctx.get_logger()


def get_secret(secret_name):
    logger.info(f"Retrieving secret: {secret_name}")

    try:
        client = boto3.client("secretsmanager")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]

        logger.info("Successfully retrieved Secret.")

        return json.loads(secret)
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {e}", exc_info=True)
        raise


def extract_data(s3_bucket, feature):
    logger.info(f"Extracting data for feature: {feature} from bucket: {s3_bucket}")

    try:
        dyf = glue_ctx.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [f"s3://{s3_bucket}/bronze/{feature}/"],
                "recurse": True,
                "groupFiles": "inPartition",
            },
            format="csv",
            format_options={
                "withHeader": True,
                "skipFirstLine": False,
                "separator": ",",
            },
        )

        row_count = dyf.count()
        if row_count == 0:
            logger.warning(f"No data found for feature: {feature}")
            return None
        logger.info(f"Extracted {row_count} rows for feature: {feature}")
        return dyf
    except Exception as e:
        logger.error(f"Data extraction failed: {e}", exc_info=True)
        raise


def transform_data(dyf):
    logger.info("Start transforming")

    try:
        now_utc = current_timestamp()

        mapped_dyf = dyf.apply_mapping(
            [
                ("Date", "string", "date", "date"),
                ("Ticker", "string", "ticker", "string"),
                ("Close", "string", "price", "double"),
                ("Volume", "string", "volume", "long"),
            ]
        )

        df = mapped_dyf.toDF()

        columns_to_check = ["date", "ticker", "price", "volume"]

        transformed_df = (
            df.na.drop(subset=columns_to_check)
            .withColumn("created_at", lit(now_utc))
            .withColumn("updated_at", lit(now_utc))
            .withColumn("price", df["price"].cast("double"))
        )

        transformed_df = transformed_df.selectExpr(
            "date",
            "ticker",
            "round(price, 2) as price",
            "volume",
            "created_at",
            "updated_at",
        )

        row_count = transformed_df.count()
        logger.info(f"Transformed dataframe with {row_count} rows.")

        return DynamicFrame.fromDF(transformed_df, glue_ctx)
    except Exception as e:
        logger.error(f"Data transformation failed: {e}", exc_info=True)
        raise


def load_data(dyf, redshift_options):
    logger.info("Loading data into Redshift")

    try:
        glue_ctx.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options=redshift_options,
            transformation_ctx="load_data",
        )
        logger.info("Data successfully loaded into Redshift.")
    except Exception as e:
        logger.error(f"Data loading failed: {e}", exc_info=True)
        raise


def main():
    job = Job(glue_ctx)
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "s3_bucket", "feature", "target_table", "redshift_url"],
    )
    job.init(args["JOB_NAME"], args)

    try:
        secret = get_secret("team3-1-redshift-access")

        redshift_options = {
            "url": args["redshift_url"],
            "dbtable": args["target_table"],
            "redshiftTmpDir": f"s3://{args['s3_bucket']}/redshift_temp/",
            "user": secret["username"],
            "password": secret["password"],
        }

        raw_dyf = extract_data(args["s3_bucket"], args["feature"])

        if raw_dyf is not None:
            transformed_dyf = transform_data(raw_dyf)
            load_data(transformed_dyf, redshift_options)
        else:
            logger.info("No data to process. Exiting job.")

        logger.info("ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed: {e}", exc_info=True)
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
