# This DAG crawls for meta data of all bonds
import json
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup

# Globals
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
S3_BUCKET = Variable.get("S3_BUCKET")
# TODO: A pre-crawler DAG for the urls
URLS_DICT = ["govt_bonds_kr", "govt_bonds_us", "corp_bonds_kr", "corp_bonds_us"]


def get_meta_data(category, bond_name, **ctxt):
    # Fetch the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(key="data/urls_bonds.json", bucket_name=S3_BUCKET)
    urls_dict = json.loads(file)

    # Bonds meta data crawling
    # TODO: ✅ Would it be better to do this on a separate DAG?
    # TODO: ✅I should try the Soup on the industry code DAG?! 🤨
    res = requests.get(urls_dict[category][bond_name]["meta"])
    time.sleep(3)
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table")  # there is only one table

    data = {}
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) == 2:
            header = cols[0].text.strip()
            content = cols[1].text.strip()
            data[header] = data.get(header, content)

    ds_year, ds_month, ds_day = "{{ ds }}"[:4], "{{ ds }}"[5:7], "{{ ds }}"[8:10]
    upload = S3CreateObjectOperator(
        task_id=f"upload_{bond_name}",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/{category}/kind={bond_name}/date={ds_year}-{ds_month}-{ds_day}/{category}_{bond_name}_meta_{ds_year}-{ds_month}.json",
        data=json.dumps(data),
        replace=True,
    )
    upload.execute(context=ctxt)

    time.sleep(3)

    return "puff"


with DAG(
    dag_id="brz_govt_bonds_meta_month",
    start_date=START_DATE,
    schedule_interval="0 0 1 * *",
    catchup=False,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
        "owner": "dee",
    },
    max_active_tasks=1,
    tags=["bronze"],
    description="All Bonds Metadata, Govt And Corp",
) as dag:
    # Dynamically generate crawling tasks
    with TaskGroup(group_id="crawler_group") as meta_data_crawler_group:
        # Put prev_task right below category loop to parallelize
        prev_task = None
        for category in URLS_DICT:
            for bond_name in URLS_DICT[category]:
                curr_task = PythonOperator(
                    task_id=f"{category}_{bond_name}",
                    python_callable=get_meta_data,
                    op_args=[category, bond_name],
                )
                if prev_task:
                    prev_task >> curr_task
                prev_task = curr_task

    meta_data_crawler_group
