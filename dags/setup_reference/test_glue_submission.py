from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from test_helpers import run_glue_crawler

"""
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/glue.html
"""

# DAG definition
default_args = {
    "retries": 0,
}

with DAG(
    dag_id="glue_submission_test",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # Glue Crawler
    run_crawler = PythonOperator(
        task_id="test_glue_crawler",
        python_callable=run_glue_crawler,
        op_kwargs={"crawler_name": "Team3-test"},
    )

    # << Transfer job script to s3 as needed

    # Glue ETL
    glue_etl_job = GlueJobOperator(
        task_id="test_glue_etl",
        job_name="Team3-1-test",
        script_location="s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/",
        aws_conn_id="aws_conn_id",
        region_name="ap-northeast-2",
    )

    # are they dependant tho?
    run_crawler >> glue_etl_job
