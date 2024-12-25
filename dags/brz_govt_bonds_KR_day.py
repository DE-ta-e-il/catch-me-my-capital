# TODO: must consider the partition cleanup .. IN OTHER DAGS
# monthly cleanup makes 'm_xxx.json' files, yearly cleanup makes 'y_xxx.json' files?
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

# NOTE: tkData seems to be the id but can't dynamically generate
# NOTE: EUR data is updated monthly, decided to forego. These are kr bonds in USD.
# TODO: What if new bonds are issued? 😑 Make a helper class for scanning and init-ing bond kinds?
BONDS = {
    "kr2024_2029": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,136391500,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202429-bond-2029-us50064fax24",
    },
    "kr2021_2031": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,114194447,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202131-bond-2031-us50064fau84",
    },
    "kr2017_2027": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,35380088,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201727-bond-2027-us50064fam68",
    },
    "kr2014_2044": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,24617525,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201444-bond-2044-us50064fal85",
    },
    "kr2005_2025": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,2321762,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/5_625-korea-republik-bond-2025-us50064fae43",
    },
    "kr2020_2030": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,57088553,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202030-bond-2030-us50064fas39",
    },
    "kr2019_2029": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,48477503,16,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201929-bond-2029-us50064faq72",
    },
    "kr2018_2048": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,43733993,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201848-bond-2048-us50064fan42",
    },
    "kr2018_2028": {
        "chart": f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData=1,43733712,1330,333&from={RANGE[0].strftime('%Y%m%d')}&to={RANGE[1].strftime('%Y%m%d')}",
        "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201828-bond-2028-us50064fap99",
    },
}


# For conditional execution
def check_first_run(*args, **ctxt):
    if FIRST_RUN:
        return "skipper"
    else:
        return "task_group2"


def get_govt_bond_data_kr(target, **ctxt):
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
                task_id=f"kr_bond_first_time_{date}",
                aws_conn_id="aws_general",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/govt_bonds_kr/kind={target}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/{target}_{date}.json",
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
    dag_id="brz_govt_bonds_KR_day",
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
                python_callable=get_govt_bond_data_kr,
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
                s3_key=f"bronze/govt_bonds_kr/kind={b}/year={ds_year}/month={ds_month}/day={ds_day}/{b}_{ds_year}-{ds_month}-{ds_day}.json",
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
