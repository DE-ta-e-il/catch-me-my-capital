import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup

# Globals
FIRST_RUN = True
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
S3_BUCKET = Variable.get("S3_BUCKET")
# Get urls parameters
# I really wanted to avoid hardcoding it ...
URLS_DICT = ["govt_bonds_kr", "govt_bonds_us", "corp_bonds_kr", "corp_bonds_us"]


# Task 1
def generate_urls(**ctxt):
    # Hooks, too, must be wrapped in here...
    # Get the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(key="data/urls_bonds.json", bucket_name=S3_BUCKET)
    urls_dict = json.loads(file)

    # Date range for the url query strings
    ds = ctxt["ds"]
    d_range = (
        (datetime(2015, 1, 1), START_DATE)
        if FIRST_RUN
        else (
            (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            datetime.strptime(ds, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
        )
    )
    # NOTE: Maximum Moody's rating for KR corp bonds are Aa2. Data points : once a day.
    # no xcom no ☠
    full_urls = {
        bond_kind: {
            bond: f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData={urls_dict[bond_kind][bond]['chart']}&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}"
            for bond in urls_dict[bond_kind]
        }
        for bond_kind in urls_dict
    }

    with open("/tmp/full_urls_bonds.json", "w") as file:
        json.dump(full_urls, file)


# Dynamic Task
def get_bond_data(bond_kind, **ctxt):
    ds = ctxt["ds"]
    date = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%m-%d")

    # Fetch urls
    with open("/tmp/full_urls_bonds.json", "r") as file:
        full_urls = json.load(file)

    # Fetching the ranged data
    for target in full_urls[bond_kind]:
        response = requests.get(full_urls[bond_kind][target])
        time.sleep(3)

        # If this is the first run,
        # unravel 10-years-worth of data
        # and save them as smaller partitions
        if FIRST_RUN:
            # gbd: Short for grouped-by-day
            gbd = defaultdict(list)
            for rec in response.json():
                date = rec["Date"]
                gbd[date[:10]].append(rec)

            if len(gbd) == 0:
                raise Exception("Nothing was fetched")

            # NOTE: Wonder if this is practical?
            # Must take parallelism into account... right? 😵
            id = 0
            for date, daily_list in gbd.items():
                upload = S3CreateObjectOperator(
                    task_id=f"kr_corp_bond_first_time_{date}_{id}",
                    aws_conn_id="aws_conn_id",
                    s3_bucket=S3_BUCKET,
                    s3_key=f"bronze/corp_bonds_kr/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
                    data=json.dumps(daily_list),
                    replace=True,
                )
                upload.execute(context=ctxt)
                id += 1
                time.sleep(0.5)
        # If it's not the first rodeo, run normal operation 🔫🤠🐂
        else:
            upload = S3CreateObjectOperator(
                task_id=f"upload_{target}",
                aws_conn_id="aws_conn_id",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/corp_bonds_kr/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
                data=json.dumps(response.json()),
                replace=True,
            )
            upload.execute(context=ctxt)

        time.sleep(2)


with DAG(
    dag_id="brz_bonds_day",
    start_date=START_DATE,
    schedule_interval="0 0 * * *",
    catchup=not FIRST_RUN,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
        "owner": "dee",
    },
    max_active_tasks=2,
    tags=["bronze"],
    description="Bonds of Korea and US, State and Corporate. Expandable.",
) as dag:
    # Generates full url from parameters
    url_generator = PythonOperator(
        task_id="bonds_url_generator",
        python_callable=generate_urls,
        provide_context=True,
    )

    # bond_kind tasks can parallelize
    with TaskGroup(group_id="api_caller_group") as api_caller_group:
        prev_task = None
        id = 0
        for bond_kind in URLS_DICT:
            curr_task = PythonOperator(
                task_id=f"fetch_{bond_kind}_{id}",
                python_callable=get_bond_data,
                provide_context=True,
                op_args=[bond_kind],
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task
            id += 1

    url_generator >> api_caller_group
