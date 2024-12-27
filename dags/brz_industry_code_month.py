# This DAG is for collecting industry codes for KOSPI, KOSDAQ, and the DICS standard
# This DAG does full refresh every month.
# TODO: Use airflow.models.connection to manage connections once AWS secrets manager is utilized.

import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from bs4 import BeautifulSoup

# Globals
# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
S3_BUCKET = Variable.get("S3_BUCKET")


# Task1
# For JSON direct response APIs
def fetch_industry_codes(market, referer, mktId, **ctxt):
    date = ctxt["ds"]
    # kospi
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    try:
        res = requests.post(
            url=url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
                "Referer": f"http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId={referer}",
            },
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
                "locale": "ko_KR",
                "mktId": mktId,
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

    # Upload
    ds_year, ds_month, ds_day = date[:4], date[5:7], date[8:10]
    upload_kospi = S3CreateObjectOperator(
        task_id=f"upload_{market}",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/industry_code/date={ds_year}-{ds_month}-{ds_day}/{market}_codes_{ds_year}-{ds_month}.json",
        data=f"{new_items}",
        replace=True,
    )
    upload_kospi.execute(context=ctxt)


# Task 2
# For crawling
def crawl_industry_codes(**ctxt):
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

    date = "{{ ds }}"
    date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    # Upload
    ds_year, ds_month, ds_day = date[:4], date[5:7], date[8:10]
    upload_gics = S3CreateObjectOperator(
        task_id="upload_gics",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/industry_code/date={ds_year}-{ds_month}-{ds_day}/gics_codes_{ds_year}-{ds_month}.json",
        data=f"{[sectors, industry_group, industry, sub_industry]}",
        replace=True,
    )
    upload_gics.execute(context=ctxt)


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
        python_callable=fetch_industry_codes,
        op_args=["kospi", "MDC0201020101", "STK"],
    )

    kosdaq_codes_fetcher = PythonOperator(
        task_id="kosdaq_industry_codes",
        python_callable=fetch_industry_codes,
        op_args=["kosdaq", "MDC0201020506", "KSQ"],
    )

    gics_codes_fetcher = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=crawl_industry_codes,
    )

    kospi_codes_fetcher >> kosdaq_codes_fetcher >> gics_codes_fetcher
