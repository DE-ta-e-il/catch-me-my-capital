# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.

import os
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup

# Globals
S3_BUCKET = os.getenv("S3_BUCKET")
DOWNLOAD_PATH = "/tmp"
GICS = "/tmp/c_gics.json"


# Task1
def kospi_industry_codes(**ctxt):
    date = ctxt["ds"]
    # kospi
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    try:
        res = requests.post(
            url=url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
                "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
            },
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
                "locale": "ko_KR",
                "mktId": "STK",
                "trdDd": datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d"),
                "money": 1,
                "csvxls_isNo": "false",
            },
        )
    except Exception as e:
        raise Exception(e)

    # It's real slo fo sho
    time.sleep(15)

    content = res.json()
    items = []
    for block in content:
        items.extend(content[block])

    new_items = []
    for item in items:
        if isinstance(item, dict):
            new_items.append(
                {
                    "item_code": item["ISU_SRT_CD"],
                    "item_name": item["ISU_ABBRV"],
                    "industry_code": item["IDX_IND_NM"],
                }
            )

    if len(new_items) == 0:
        raise Exception("NOPE NOT GETTING ANY")

    ctxt["ti"].xcom_push(key="kospi", value=new_items)

    return "puff"


# Task2
def kosdaq_industry_codes(**ctxt):
    date = ctxt["ds"]
    # kosdaq
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    try:
        res = requests.post(
            url=url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
                "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506",
            },
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
                "locale": "ko_KR",
                "mktId": "KSQ",
                "trdDd": datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d"),
                "money": 1,
                "csvxls_isNo": "false",
            },
        )
    except Exception as e:
        raise Exception(e)

    time.sleep(15)

    content = res.json()
    items = []
    for block in content:
        items.extend(content[block])

    new_items = []
    for item in items:
        if isinstance(item, dict):
            new_items.append(
                {
                    "item_code": item["ISU_SRT_CD"],
                    "item_name": item["ISU_ABBRV"],
                    "industry_code": item["IDX_IND_NM"],
                }
            )

    if len(new_items) == 0:
        raise Exception("NOPE NOT GETTING ANY")

    ctxt["ti"].xcom_push(key="kosdaq", value=new_items)

    return "puff"


# Task 3
def gics_industry_codes(**ctxt):
    url = "https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard#Classification"
    res = requests.get(url)
    time.sleep(3)
    soup = BeautifulSoup(res.text, "html.parser")
    time.sleep(2)

    rows = soup.find_all("td")

    # Industry codes lengths are 2, 4, 6, 8, and
    # each category code acts as the prefix(reference key) of the prior(higher) category
    # so it would be safe to devide each category into tables
    # hence the logic.
    # https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard#Classification
    sectors, industry_group, industry, sub_industry = {}, {}, {}, {}
    for i, r in enumerate(rows):
        if i % 2 == 0:  # Even indices indicate the name of the previous odd indices
            target = r.text.strip()
            name = rows[i + 1].text.strip()
            if len(target) == 2:
                sectors[target] = sectors.get(target, name)
            elif len(target) == 4:
                industry_group[target] = industry_group.get(target, name)
            elif len(target) == 6:
                industry[target] = industry.get(target, name)
            else:
                sub_industry[target] = sub_industry.get(target, name)

    # save
    # indents of 4
    # with open(GICS, "w") as f:
    #     json.dump([sectors, industry_group, industry, sub_industry], f, indent=4)

    # with open(GICS, "r") as f:
    #     CURR_FILE = f.read()

    ctxt["ti"].xcom_push(
        key="gics", value=[sectors, industry_group, industry, sub_industry]
    )
    return "puff"


with DAG(
    dag_id="brz_industry_code_month",
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
    },
    max_active_tasks=1,
) as dag:
    task01 = PythonOperator(
        task_id="kospi_industry_codes",
        python_callable=kospi_industry_codes,
    )

    task02 = PythonOperator(
        task_id="kosdaq_industry_codes",
        python_callable=kosdaq_industry_codes,
    )

    task03 = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=gics_industry_codes,
    )

    with TaskGroup(group_id="s3_group") as task_group1:
        prev_task = None
        ds_year, ds_month = "{{ ds[:4] }}", "{{ ds[5:7] }}"
        for scope in ["kosdaq", "kospi", "gics"]:
            curr_task = S3CreateObjectOperator(
                task_id=f"upload_{scope}",
                aws_conn_id="aws_conn_id",
                s3_bucket=S3_BUCKET,
                s3_key=f"bronze/industry_code/year={ds_year}/month={ds_month}/{scope}_codes_{ds_year}-{ds_month}.json",
                data="{{ task_instance.xcom_pull(task_ids='"
                + scope
                + "_industry_codes', key='"
                + scope
                + "') }}",
                replace=True,
            )
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task

    task01 >> task02 >> task03 >> task_group1
