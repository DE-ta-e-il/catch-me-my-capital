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
    "kr2024_2029",
    "kr2021_2031",
    "kr2017_2027",
    "kr2014_2044",
    "kr2005_2025",
    "kr2020_2030",
    "kr2019_2029",
    "kr2018_2048",
    "kr2018_2028",
]


# NOTE: tkData seems to be the id but can't dynamically generate
# NOTE: EUR data is updated monthly, decided to forego. These are kr bonds in USD.
# TODO: What if new bonds are issued? 😑 Make a helper class for scanning and init-ing bond kinds?
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
        "kr2024_2029": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,136391500,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202429-bond-2029-us50064fax24",
        },
        "kr2021_2031": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,114194447,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202131-bond-2031-us50064fau84",
        },
        "kr2017_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,35380088,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201727-bond-2027-us50064fam68",
        },
        "kr2014_2044": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,24617525,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201444-bond-2044-us50064fal85",
        },
        "kr2005_2025": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,2321762,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/5_625-korea-republik-bond-2025-us50064fae43",
        },
        "kr2020_2030": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,57088553,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202030-bond-2030-us50064fas39",
        },
        "kr2019_2029": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,48477503,16,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201929-bond-2029-us50064faq72",
        },
        "kr2018_2048": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,43733993,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201848-bond-2048-us50064fan42",
        },
        "kr2018_2028": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,43733712,1330,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201828-bond-2028-us50064fap99",
        },
    }

    with open("/tmp/govt_bonds_kr_urls.json", "w") as json_file:
        json.dump(bonds, json_file)

    return "puff"


# Task 2
def get_govt_bond_data_kr(target, **ctxt):
    # load up the urls
    with open("/tmp/govt_bonds_kr_urls.json", "r") as json_file:
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
                task_id=f"kr_bond_first_time_{date}",
                aws_conn_id="aws_conn_id",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/govt_bonds_kr/kind={target}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/{target}_{date}.json",
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
            s3_key=f"bronze/govt_bonds_kr/kind={target}/year={ds_year}/month={ds_month}/day={ds_day}/{target}_{ds_year}-{ds_month}-{ds_day}.json",
            data=json.dumps(response.json()),
            replace=True,
        )
        upload.execute(context=ctxt)

    time.sleep(2)

    return "puff"


with DAG(
    dag_id="brz_govt_bonds_KR_day",
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
    description="Government Bonds, KR",
) as dag:
    # URL generation with range
    generate_url = PythonOperator(
        task_id="govt_bonds_kr_url_generator", python_callable=url_generator
    )

    # Dynamically generate api tasks
    with TaskGroup(group_id="api_caller_group3") as api_caller_group:
        prev_task = None
        for b in BOND_KINDS:
            curr_task = PythonOperator(
                task_id=f"govt_bonds_kr_{b}",
                python_callable=get_govt_bond_data_kr,
                op_args=[b],
                provide_context=True,
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    generate_url >> api_caller_group
