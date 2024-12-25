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

# NOTE: Maximum Moody's rating for KR corp bonds are Aa2. Data points : once a day.
BONDS = {
    "EIBK2016_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,34831838,203,88&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/export-import_bk_of_korea-_thead-medium-term_notes_201627-bond-2027-au3cb0241248",
    },
    "SHBK2018_2028": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,43398551,16,88&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/shinhan_bank_co-bond-2028-au3cb0256113",
    },
    "KNOL2020_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,55499207,14,1&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea_national_oil_corpsf-medium-term_notes_202027-bond-2027-ch0554992070",
    },
    "KWRS2023_2025": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,127136063,4,1&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea_water_resources_corpsf-medium-term_notes_202325-bond-2025-ch1271360633",
    },
    "KELP1997_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,946178,13,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/7_000-korea-electric-power-bond-2027-us500631ah98",
    },
    "KDVB2020_2026": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,57798367,14,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea_development_bank-_thedl-notes_202026-bond-2026-us500630de57",
    },
    "KEXP2015_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,49624867,13,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea_expressway_corpdl-medium-term_notes_201527-bond-2027-xs1203861403",
    },
}


# For conditional execution
def check_first_run(*args, **ctxt):
    if FIRST_RUN:
        return "skipper"
    else:
        return "task_group2"


def get_corp_bond_data_kr(target, **ctxt):
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
                task_id=f"kr_corp_bond_first_time_{date}",
                aws_conn_id="aws_general",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/corp_bonds_kr/kind={target}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/{target}_{date}.json",
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
    dag_id="brz_corp_bonds_KR_day",
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
                task_id=f"corp_bonds_kr_{b}",
                python_callable=get_corp_bond_data_kr,
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
                s3_key=f"bronze/corp_bonds_kr/kind={b}/year={ds_year}/month={ds_month}/day={ds_day}/{b}_{ds_year}-{ds_month}-{ds_day}.json",
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
