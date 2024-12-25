import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests  # NOTE: Ruff is making this stick to the airflow group. Why?
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

# NOTE: Selected only the ones that have Moody's rating of Aaa. Data points : once a day.
# TODO: What if new bonds are issued? 😑 Make a helper class for scanning and init-ing bond kinds?
# TODO: What happens when a bond matures??
BONDS = {
    "us1995_2025": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,346171,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/7_625-us-staatsanleihen-bond-2025-us912810et17",
    },
    "us2021_2041": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,114723606,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-bonds_202141-bond-2041-us912810tc27",
    },
    "us2020_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,52098446,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202027-bond-2027-us912828z781",
    },
    "us2012_2042": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,14880162,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/3_125-us-staatsanleihen-bond-2042-us912810qu51",
    },
    "us2013_2043": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,20631489,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201343-bond-2043-us912810qz49",
    },
    "us2015_2045": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,29134684,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201545-bond-2045-us912810rn00",
    },
    "us2020_2030": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,56362275,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202030-bond-2030-us91282cae12",
    },
    "us2022_2025": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,121526148,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202225-bond-2025-us91282cfk27",
    },
    "us2023_2030": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,125859665,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202330-bond-2030-us91282cgs44",
    },
    "us2023_2026": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,127546855,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202326-bond-2026-us91282chh79",
    },
    "us2023_2028": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,128428826,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202328-bond-2028-us91282chq78",
    },
    "us2019_2026": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,50082052,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201926-bond-2026-us912828yg91",
    },
    "us2018_2025": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,40982781,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201825-bond-2025-us9128284f40",
    },
}


# For conditional execution
def check_first_run(*args, **ctxt):
    if FIRST_RUN:
        return "skipper"
    else:
        return "task_group2"


def get_govt_bond_data_us(target, **ctxt):
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
                task_id=f"us_bond_first_time_{date}",
                aws_conn_id="aws_general",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/govt_bonds_us/kind={target}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/{target}_{date}.json",
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
    dag_id="brz_govt_bonds_US_day",
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
                task_id=f"govt_bonds_{b}",
                python_callable=get_govt_bond_data_us,
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
                s3_key=f"bronze/govt_bonds_us/kind={b}/year={ds_year}/month={ds_month}/day={ds_day}/{b}_{ds_year}-{ds_month}-{ds_day}.json",
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
