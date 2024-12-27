import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
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
    "APPL2016_2026",
    "NYLF2019_2027",
    "JOHN1999_2029",
    "MSCP2016_2026",
    "TNVA1995_2025",
]


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
    # NOTE: Maximum Moody's rating for US corp bonds are Aaa. Data points : once a day.
    bonds = {
        "APPL2016_2026": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,32823537,1330,88&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/apple_incad-notes_201626-bond-2026-au3cb0237881",
        },
        "NYLF2019_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,47129795,14,1&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/new_york_life_global_fundingsf-medium-term_notes_201927-bond-2027-ch0471297959",
        },
        "JOHN1999_2029": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,910470,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/6_950-johnson-johnson-bond-2029-us478160aj37",
        },
        "MSCP2016_2026": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,33507336,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/microsoft_corpdl-notes_201616-26-bond-2026-us594918br43",
        },
        "TNVA1995_2025": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,73366,4,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/6_750-tennessee-valley-authority-bond-2025-us880591cj98",
        },
    }

    with open("/tmp/corp_bonds_us_urls.json", "w") as json_file:
        json.dump(bonds, json_file)

    return "puff"


# Task 2
def get_corp_bond_data_us(target, **ctxt):
    # load up the urls
    with open("/tmp/corp_bonds_us_urls.json", "r") as json_file:
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
                task_id=f"us_corp_bond_first_time_{date}",
                aws_conn_id="aws_conn_id",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/corp_bonds_us/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
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
            s3_key=f"bronze/corp_bonds_us/kind={target}/date={ds_year}-{ds_month}-{ds_day}/{target}_{ds_year}-{ds_month}-{ds_day}.json",
            data=json.dumps(response.json()),
            replace=True,
        )
        upload.execute(context=ctxt)

    time.sleep(2)

    return "puff"


with DAG(
    dag_id="brz_corp_bonds_US_day",
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
    description="Corporate Bonds, US",
) as dag:
    generate_url = PythonOperator(
        task_id="corp_bonds_us_url_generator", python_callable=url_generator
    )

    # Dynamically generate api tasks
    with TaskGroup(group_id="api_caller_group2") as api_caller_group:
        prev_task = None
        for b in BOND_KINDS:
            curr_task = PythonOperator(
                task_id=f"corp_bonds_us_{b}",
                python_callable=get_corp_bond_data_us,
                op_args=[b],
                provide_context=True,
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    generate_url >> api_caller_group
