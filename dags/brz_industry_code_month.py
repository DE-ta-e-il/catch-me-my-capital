# This DAG is for collecting industry codes for KOSPI, KOSDAQ, S&P, MSCI
# This DAG does full refresh every month.
# TODO: /tmp cleanup

import json
import os
import time
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

import plugins.helpers as helpers

# Constants
S3_BUCKET = "team3-1-s3"
DOWNLOAD_PATH = "/tmp"
S3 = boto3.client("s3")
GICS = "/tmp/c_gics.json"


# This uses the same page, hence one task two jobs
def kospi_and_kosdaq_industry_codes(**ctxt):
    url = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506"
    driver = helpers.get_chrome_driver(DOWNLOAD_PATH)
    driver.get(url)
    time.sleep(5)

    # kospi
    # button = driver.find_element("css selector", "button.CI-MDI-UNIT-DOWNLOAD")
    # button.click()
    rows = driver.find_elements("css selector", "tr.CI-GRID-ODD")
    evenrows = driver.find_elements("css selector", "tr.CI-GRID-EVEN")
    rows.extend(evenrows)
    for r in rows:
        r_txt = r.text.split()
        r_txt[0]

    target_path = helpers.rename_downloaded(DOWNLOAD_PATH, "kospi")
    ctxt["ti"].xcom_push(key="kospi", value=target_path)

    # kosdaq
    radio = driver.find_element("css selector", "label[for='mktId_0_2']")
    radio.click()
    time.sleep(0.5)

    search = driver.find_element("css selector", "a#jsSearchButton")
    search.click()
    time.sleep(2)

    # button.click()
    time.sleep(2)

    target_path = helpers.rename_downloaded(DOWNLOAD_PATH, "kosdaq")
    ctxt["ti"].xcom_push(key="kosdaq", value=target_path)

    driver.quit()
    return "puff"


# GICS codes from wikipedia
def gics_industry_codes(**ctxt):
    url = "https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard#Classification"
    driver = helpers.get_chrome_driver(DOWNLOAD_PATH)
    driver.get(url)
    time.sleep(2)

    rows = driver.find_elements("tag name", "td")

    # Industry codes lengths are 2, 4, 6, 8, and
    # each category code acts as the prefix(reference key) of the prior(higher) category
    # so it would be safe to devide each category into tables
    # hence the logic.
    # https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard#Classification
    sectors, industry_group, industry, sub_industry = {}, {}, {}, {}
    for i, r in enumerate(rows):
        if i % 2 == 0:  # Even indices indicate the name of the previous odd indices
            target = r.text.strip()
            if len(target) == 2:
                sectors[target] = sectors.get(target, rows[i + 1])
            elif len(target) == 4:
                industry_group[target] = industry_group.get(target, rows[i + 1])
            elif len(target) == 6:
                industry[target] = industry.get(target, rows[i + 1])
            else:
                sub_industry[target] = sub_industry.get(target, rows[i + 1])

    # save
    # indents of 4
    with open(GICS, "w") as f:
        json.dump([sectors, industry_group, industry, sub_industry], f, indent=4)

    return "puff"


# 에스쭈리
def upload_to_s3(**ctxt):
    execution_date = ctxt["execution_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")

    files_to_upload = [
        ctxt["ti"].xcom_pull(key="kospi", task_ids="kospi_kosdaq_industry_codes"),
        ctxt["ti"].xcom_pull(key="kosdaq", task_ids="kospi_kosdaq_industry_codes"),
        GICS,
    ]

    for f in files_to_upload:
        S3.upload_file(
            f,
            S3_BUCKET,
            f"bronze/industry_code/year={year}/month={month}/{os.path.basename(f)}",
        )
        os.remove(f)

    return "puff"


with DAG(
    dag_id="brz_industry_code_month",
    start_date=datetime(2024, 11, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    default_args={
        "retries": 0,
    },
    max_active_tasks=1,
) as dag:
    task01 = PythonOperator(
        task_id="kospi_kosdaq_industry_codes",
        python_callable=kospi_and_kosdaq_industry_codes,
    )

    task02 = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=gics_industry_codes,
    )

    task03 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    task01 >> task02 >> task03
