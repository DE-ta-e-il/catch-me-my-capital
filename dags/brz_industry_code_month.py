# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.

import os
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
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

    # Upload
    ds_year, ds_month = "{{ ds[:4] }}", "{{ ds[5:7] }}"
    upload_kospi = S3CreateObjectOperator(
        task_id="upload_kospi",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/industry_code/year={ds_year}/month={ds_month}/kospi_codes_{ds_year}-{ds_month}.json",
        data=f"{new_items}",
        replace=True,
    )
    upload_kospi.execute(context=ctxt)

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

    # Upload
    ds_year, ds_month = "{{ ds[:4] }}", "{{ ds[5:7] }}"
    upload_kosdaq = S3CreateObjectOperator(
        task_id="upload_kosdaq",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/industry_code/year={ds_year}/month={ds_month}/kosdaq_codes_{ds_year}-{ds_month}.json",
        data=f"{new_items}",
        replace=True,
    )
    upload_kosdaq.execute(context=ctxt)

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

    # Upload
    ds_year, ds_month = "{{ ds[:4] }}", "{{ ds[5:7] }}"
    upload_gics = S3CreateObjectOperator(
        task_id="upload_gics",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/industry_code/year={ds_year}/month={ds_month}/gics_codes_{ds_year}-{ds_month}.json",
        data=f"{[sectors, industry_group, industry, sub_industry]}",
        replace=True,
    )
    upload_gics.execute(context=ctxt)

    return "puff"


with DAG(
    dag_id="brz_industry_code_month",
    start_date=datetime(2024, 12, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    tags=["bronze"],
    description="A DAG that fetches industry(sector) codes for stocks.",
    default_args={"retries": 0, "trigger_rule": "all_success", "owner": "dee"},
    max_active_tasks=1,
) as dag:
    kospi_codes_fetcher = PythonOperator(
        task_id="kospi_industry_codes",
        python_callable=kospi_industry_codes,
    )

    kosdaq_codes_fetcher = PythonOperator(
        task_id="kosdaq_industry_codes",
        python_callable=kosdaq_industry_codes,
    )

    gics_codes_fetcher = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=gics_industry_codes,
    )

    kospi_codes_fetcher >> kosdaq_codes_fetcher >> gics_codes_fetcher
