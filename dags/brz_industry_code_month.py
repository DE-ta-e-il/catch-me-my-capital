# This DAG is for collecting industry codes for KOSPI, KOSDAQ, S&P, MSCI
# This DAG does full refresh every month.
# TODO: /tmp cleanup

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

import plugins.helpers as helpers

# Globals
S3_BUCKET = "team3-1-s3"
DOWNLOAD_PATH = "/tmp"
GICS = "/tmp/c_gics.json"


#### THIS IS DISCONTINUED
# Can't scroll to bottom, can't get all the stuff
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
    start_date=datetime(2024, 11, 1),
    schedule_interval="0 0 1 * *",
    catchup=False,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
    },
    max_active_tasks=1,
) as dag:
    # task01 = PythonOperator(
    #     task_id="kospi_kosdaq_industry_codes",
    #     python_callable=kospi_and_kosdaq_industry_codes,
    # )

    task02 = PythonOperator(
        task_id="gics_industry_codes",
        python_callable=gics_industry_codes,
    )

    task03 = S3CreateObjectOperator(
        task_id="gics_to_s3",
        aws_conn_id="aws_general",
        s3_bucket=S3_BUCKET,
        s3_key="bronze/industry_code/year={{ ds[:4] }}/month={{ ds[5:7] }}/c_gics.json",
        data="{{ task_instance.xcom_pull(task_ids='gics_industry_codes', key='gics') }}",
        replace=True,
    )

    task02 >> task03
