import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup

# Globals
FIRST_RUN = True
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
RANGE = (
    (datetime(2015, 1, 1), START_DATE)
    if FIRST_RUN
    else (
        (datetime.strptime("{{ ds }}", "%Y-%m-%d") - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ),
        datetime.strptime("{{ ds }}", "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0
        ),
    )
)
S3_BUCKET = "team3-1-s3"

# NOTE: Maximum Moody's rating for US corp bonds are Aaa. Data points : once a day.
BONDS = {
    "APPL2016_2026": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,32823537,1330,88&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/apple_incad-notes_201626-bond-2026-au3cb0237881",
    },
    "NYLF2019_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,47129795,14,1&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/new_york_life_global_fundingsf-medium-term_notes_201927-bond-2027-ch0471297959",
    },
    "JOHN1999_2029": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,910470,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/6_950-johnson-johnson-bond-2029-us478160aj37",
    },
    "MSCP2016_2026": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,33507336,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/microsoft_corpdl-notes_201616-26-bond-2026-us594918br43",
    },
    "TNVA1995_2025": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,73366,4,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/6_750-tennessee-valley-authority-bond-2025-us880591cj98",
    },
}


# For conditional execution
def check_first_run(*args, **ctxt):
    if FIRST_RUN:
        return "skipper"
    else:
        return "task_group2"


def get_corp_bond_data_us(target, **ctxt):
    # the daily data
    response = requests.get(BONDS[target]["chart"])
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
        for date, daily_list in gbd.items():
            upload = S3CreateObjectOperator(
                task_id=f"us_corp_bond_first_time_{date}",
                aws_conn_id="aws_general",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/corp_bonds_us/kind={target}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/{target}_{date}.json",
                data=json.dumps(daily_list),
                replace=True,
            )
            upload.execute(context=ctxt)
            time.sleep(0.5)
    # Normal operation
    else:
        ctxt["ti"].xcom_push(key=target, value=response.json())

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
    },
    max_active_tasks=1,
) as dag:
    # Conditional execution
    brancher = BranchPythonOperator(task_id="brancher", python_callable=check_first_run)

    # Dynamically generate api tasks
    with TaskGroup(group_id="task_group1") as task_group1:
        prev_task = None
        for b in BONDS:
            curr_task = PythonOperator(
                task_id=f"corp_bonds_us_{b}",
                python_callable=get_corp_bond_data_us,
                op_args=[b],
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    # Upload tasks
    with TaskGroup(group_id="task_group2") as task_group2:
        prev_task = None
        ds_year, ds_month, ds_day = "{{ ds[:4] }}", "{{ ds[5:7] }}", "{{ ds[8:10] }}"
        for b in BONDS:
            curr_task = S3CreateObjectOperator(
                task_id=f"upload_{b}",
                aws_conn_id="aws_general",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/corp_bonds_us/kind={b}/year={ds_year}/month={ds_month}/day={ds_day}/{b}_{ds_year}-{ds_month}-{ds_day}.json",
                data="{{ task_instance.xcom_pull(task_ids='upload_"
                + b
                + "', key='"
                + b
                + "') }}",
                replace=True,
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    skipper = DummyOperator(task_id="skipper")

    task_group1 >> brancher >> [skipper, task_group2]
