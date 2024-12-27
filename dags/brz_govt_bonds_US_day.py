import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests  # NOTE: Ruff is making this stick to the airflow group. Why?
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup

# Globals
FIRST_RUN = True
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
S3_BUCKET = os.getenv("S3_BUCKET")
BOND_KINDS = [
    "us1995_2025",
    "us2021_2041",
    "us2020_2027",
    "us2012_2042",
    "us2013_2043",
    "us2015_2045",
    "us2020_2030",
    "us2022_2025",
    "us2023_2030",
    "us2023_2026",
    "us2023_2028",
    "us2019_2026",
    "us2018_2025",
]


# NOTE: Selected only the ones that have Moody's rating of Aaa. Data points : once a day.
# TODO: What if new bonds are issued? 😑 Make a helper class for scanning and init-ing bond kinds?
# TODO: What happens when a bond matures??
# Task 1
def url_generator(**ctxt):
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
    # NOTE: Maximum Moody's rating for KR govt bonds are Aa2. Data points : once a day.
    bonds = {
        "us1995_2025": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,346171,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/7_625-us-staatsanleihen-bond-2025-us912810et17",
        },
        "us2021_2041": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,114723606,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-bonds_202141-bond-2041-us912810tc27",
        },
        "us2020_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,52098446,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202027-bond-2027-us912828z781",
        },
        "us2012_2042": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,14880162,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/3_125-us-staatsanleihen-bond-2042-us912810qu51",
        },
        "us2013_2043": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,20631489,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201343-bond-2043-us912810qz49",
        },
        "us2015_2045": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,29134684,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201545-bond-2045-us912810rn00",
        },
        "us2020_2030": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,56362275,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202030-bond-2030-us91282cae12",
        },
        "us2022_2025": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,121526148,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202225-bond-2025-us91282cfk27",
        },
        "us2023_2030": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,125859665,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202330-bond-2030-us91282cgs44",
        },
        "us2023_2026": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,127546855,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202326-bond-2026-us91282chh79",
        },
        "us2023_2028": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,128428826,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202328-bond-2028-us91282chq78",
        },
        "us2019_2026": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,50082052,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201926-bond-2026-us912828yg91",
        },
        "us2018_2025": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,40982781,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201825-bond-2025-us9128284f40",
        },
    }

    with open("/tmp/govt_bonds_us_urls.json", "w") as json_file:
        json.dump(bonds, json_file)

    return "puff"


# Task 2
def get_govt_bond_data_us(target, **ctxt):
    # load up the urls
    with open("/tmp/govt_bonds_us_urls.json", "r") as json_file:
        bonds = json.load(json_file)

    # the daily data
    response = requests.get(bonds[target]["chart"])
    time.sleep(3)

    if FIRST_RUN:  # unravel the bunch
        # short for grouped-by-day
        gbd = defaultdict(list)
        for rec in response.json():
            date = rec["Date"]
            gbd[date[:10]].append(rec)

        if len(gbd) == 0:
            raise Exception("Nothing was fetched")

        for date, daily_list in gbd.items():
            upload = S3CreateObjectOperator(
                task_id=f"us_bond_first_time_{date}",
                aws_conn_id="aws_conn_id",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/govt_bonds_us/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
                data=json.dumps(daily_list),
                replace=True,
            )
            upload.execute(context=ctxt)
            time.sleep(0.5)
    # Normal operation
    else:
        ds_year, ds_month, ds_day = "{{ ds }}"[:4], "{{ ds }}"[5:7], "{{ ds }}"[8:10]
        upload = S3CreateObjectOperator(
            task_id=f"upload_{target}",
            aws_conn_id="aws_conn_id",
            s3_bucket=S3_BUCKET,
            s3_key=f"bronze/govt_bonds_us/kind={target}/date={ds_year}-{ds_month}-{ds_day}/{target}_{ds_year}-{ds_month}-{ds_day}.json",
            data=json.dumps(response.json()),
            replace=True,
        )
        upload.execute(context=ctxt)

    time.sleep(2)

    return "puff"


with DAG(
    dag_id="brz_govt_bonds_US_day",
    start_date=START_DATE,
    schedule_interval="0 0 * * *",
    catchup=not FIRST_RUN,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
        "owner": "dee",
    },
    max_active_tasks=1,
    tags=["bronze"],
    description="Government Bonds, US",
) as dag:
    # URL generation with range
    generate_url = PythonOperator(
        task_id="govt_bonds_us_url_generator", python_callable=url_generator
    )

    # Dynamically generate api tasks
    with TaskGroup(group_id="api_caller_group4") as api_caller_group:
        prev_task = None
        for b in BOND_KINDS:
            curr_task = PythonOperator(
                task_id=f"govt_bonds_us_{b}",
                python_callable=get_govt_bond_data_us,
                op_args=[b],
                provide_context=True,
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    generate_url >> api_caller_group
