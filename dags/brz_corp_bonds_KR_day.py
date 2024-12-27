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
    "EIBK2016_2027",
    "SHBK2018_2028",
    "KNOL2020_2027",
    "KWRS2023_2025",
    "KELP1997_2027",
    "KDVB2020_2026",
    "KEXP2015_2027",
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
    # NOTE: Maximum Moody's rating for KR corp bonds are Aa2. Data points : once a day.
    bonds = {
        "EIBK2016_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,34831838,203,88&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/export-import_bk_of_korea-_thead-medium-term_notes_201627-bond-2027-au3cb0241248",
        },
        "SHBK2018_2028": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,43398551,16,88&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/shinhan_bank_co-bond-2028-au3cb0256113",
        },
        "KNOL2020_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,55499207,14,1&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea_national_oil_corpsf-medium-term_notes_202027-bond-2027-ch0554992070",
        },
        "KWRS2023_2025": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,127136063,4,1&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea_water_resources_corpsf-medium-term_notes_202325-bond-2025-ch1271360633",
        },
        "KELP1997_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,946178,13,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/7_000-korea-electric-power-bond-2027-us500631ah98",
        },
        "KDVB2020_2026": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,57798367,14,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea_development_bank-_thedl-notes_202026-bond-2026-us500630de57",
        },
        "KEXP2015_2027": {
            "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,49624867,13,333&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}",
            "meta": "https://markets.businessinsider.com/bonds/korea_expressway_corpdl-medium-term_notes_201527-bond-2027-xs1203861403",
        },
    }

    with open("/tmp/corp_bonds_kr_urls.json", "w") as json_file:
        json.dump(bonds, json_file)

    return "puff"


# Task 2
def get_corp_bond_data_kr(target, **ctxt):
    # load up the urls
    with open("/tmp/corp_bonds_kr_urls.json", "r") as json_file:
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

        # NOTE: Wonder if this is practical?
        # NOTE: Can't execute from within a loop?

        for date, daily_list in gbd.items():
            upload = S3CreateObjectOperator(
                task_id=f"kr_corp_bond_first_time_{date}",
                aws_conn_id="aws_conn_id",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/corp_bonds_kr/kind={target}/date={date[:4]}-{date[5:7]}-{date[8:10]}/{target}_{date}.json",
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
            s3_key=f"bronze/corp_bonds_kr/kind={target}/date={ds_year}-{ds_month}-{ds_day}/{target}_{ds_year}-{ds_month}-{ds_day}.json",
            data=json.dumps(response.json()),
            replace=True,
        )
        upload.execute(context=ctxt)

    time.sleep(2)

    return "puff"


with DAG(
    dag_id="brz_corp_bonds_KR_day",
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
    description="Corporate Bonds, Korea",
) as dag:
    # Generate bonds url
    generate_url = PythonOperator(
        task_id="corp_bonds_kr_url_generator", python_callable=url_generator
    )

    # Dynamically generate api tasks
    with TaskGroup(group_id="api_caller_group") as api_caller_group:
        prev_task = None
        for b in BOND_KINDS:
            curr_task = PythonOperator(
                task_id=f"corp_bonds_kr_{b}",
                python_callable=get_corp_bond_data_kr,
                op_args=[b],
                provide_context=True,
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    generate_url >> api_caller_group
