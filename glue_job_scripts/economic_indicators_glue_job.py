import json
import sys
from functools import reduce

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

glue_ctx = GlueContext(SparkContext.getOrCreate())
logger = glue_ctx.get_logger()

DATABASE_NAME = "team3-db"
REDSHIFT_TEMP_DIR = "redshift_temp/"

STAT_CODE_TABLE_TEMPLATE = "{db_schema}.dim_stat_code"
ECONOMIC_INDICATORS_TABLE_TEMPLATE = "{db_schema}.fact_economic_indicators"


def get_secret(secret_name):
    logger.info(f"Retrieving secret: {secret_name}")

    try:
        client = boto3.client("secretsmanager")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]

        logger.info("Successfully retrieved Secret.")

        return json.loads(secret)
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {e}")
        raise


def extract_data(database, tables):
    table_names = ", ".join(tables)

    logger.info(f"Extracting data for: {table_names} from: {database}")
    dfs = []

    try:
        for table in tables:
            df = glue_ctx.create_dynamic_frame.from_catalog(
                database=database, table_name=table
            ).toDF()

            frequency = table.split("_")[0]

            df = df.withColumn("frequency", lit(frequency))

            dfs.append(df)

        unioned_df = reduce(DataFrame.unionAll, dfs)
        row_count = unioned_df.count()
        if row_count == 0:
            logger.warning(f"No data found for: {table_names}")
            return None

        logger.info(f"Extracted {row_count} rows for: {table_names}")
        return unioned_df
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise


def transform_data(df, transform_function, columns, reference_data=None):
    logger.info("Start transforming data")
    now_utc = current_timestamp()

    try:
        transformed_df = None

        if reference_data:
            transformed_df = transform_function(df, reference_data)
        else:
            transformed_df = transform_function(df)

        transformed_df = (
            transformed_df.withColumn("created_at", lit(now_utc)).withColumn(
                "updated_at", lit(now_utc)
            )
        ).select(*columns)

        row_count = transformed_df.count()
        logger.info(f"Transformation successful: {row_count} rows transformed.")

        return DynamicFrame.fromDF(transformed_df, glue_ctx)
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise


def transform_meta(df):
    transformed_df = df.selectExpr(
        "STAT_CODE AS stat_code", "STAT_NAME AS stat_name", "UNIT_NAME AS unit_name"
    ).dropDuplicates()

    return transformed_df


def transform_market_data(df, reference_df):
    transformed_df = df.join(
        reference_df,
        (reference_df["iso_alpha2"] == df["ITEM_CODE1"])
        | (reference_df["iso_alpha3"] == df["ITEM_CODE1"]),
        "left",
    )

    transformed_df = transformed_df.selectExpr(
        "TIME AS time",
        "STAT_CODE AS stat_code",
        "ITEM_CODE1 AS country_code",
        "ROUND(CAST(DATA_VALUE AS double), 2) AS value",
        "frequency",
        "CAST(ymd AS DATE) AS date",
    )

    return transformed_df


def load_data(dyf, table, redshift_conn_options):
    logger.info("Loading data into Redshift")

    try:
        glue_ctx.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options={**redshift_conn_options, "dbtable": table},
            transformation_ctx="load_data",
        )
        logger.info("Data successfully loaded into Redshift.")
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "s3_bucket",
            "redshift_url",
            "db_schema",
            "source_tables",
        ],
    )

    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)
    db_schema = args["db_schema"]
    source_tables = json.loads(args["source_tables"])

    try:
        secret = get_secret("team3-1-redshift-access")

        redshift_conn_options = {
            "url": args["redshift_url"],
            "redshiftTmpDir": f"s3://{args['s3_bucket']}/redshift_temp/",
            "user": secret["username"],
            "password": secret["password"],
        }

        raw_df = extract_data(DATABASE_NAME, source_tables)
        dim_country_code_df = glue_ctx.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                **redshift_conn_options,
                "dbtable": f"{db_schema}.dim_country_code",
            },
        ).toDF()

        if raw_df is not None:
            dim_stat_code_dyf = transform_data(
                df=raw_df,
                transform_function=transform_meta,
                columns=["stat_code", "stat_name", "unit_name", "created_at"],
            )
            fact_economic_indicators_dyf = transform_data(
                df=raw_df,
                transform_function=transform_market_data,
                reference_data=dim_country_code_df,
                columns=[
                    "time",
                    "stat_code",
                    "country_code",
                    "value",
                    "frequency",
                    "date",
                    "created_at",
                    "updated_at",
                ],
            )

            load_data(
                dim_stat_code_dyf,
                STAT_CODE_TABLE_TEMPLATE.format(db_schema=db_schema),
                redshift_conn_options,
            )
            load_data(
                fact_economic_indicators_dyf,
                ECONOMIC_INDICATORS_TABLE_TEMPLATE.format(db_schema=db_schema),
                redshift_conn_options,
            )
        else:
            logger.info("No data to process. Exiting job.")

        logger.info("ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
