# This DAG crawls for meta data of all bonds
import json
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup

# Globals
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
S3_BUCKET = os.getenv("S3_BUCKET")
# TODO: Move it to a JSON? // pre-crawl for a list -> categories & urls to json
META = {
    "govt_bonds_kr": {
        "kr2024_2029": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202429-bond-2029-us50064fax24",
        },
        "kr2021_2031": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202131-bond-2031-us50064fau84",
        },
        "kr2017_2027": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201727-bond-2027-us50064fam68",
        },
        "kr2014_2044": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201444-bond-2044-us50064fal85",
        },
        "kr2005_2025": {
            "meta": "https://markets.businessinsider.com/bonds/5_625-korea-republik-bond-2025-us50064fae43",
        },
        "kr2020_2030": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_202030-bond-2030-us50064fas39",
        },
        "kr2019_2029": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201929-bond-2029-us50064faq72",
        },
        "kr2018_2048": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201848-bond-2048-us50064fan42",
        },
        "kr2018_2028": {
            "meta": "https://markets.businessinsider.com/bonds/korea-_republikdl-notes_201828-bond-2028-us50064fap99",
        },
    },
    "govt_bonds_us": {
        "us1995_2025": {
            "meta": "https://markets.businessinsider.com/bonds/7_625-us-staatsanleihen-bond-2025-us912810et17",
        },
        "us2021_2041": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-bonds_202141-bond-2041-us912810tc27",
        },
        "us2020_2027": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202027-bond-2027-us912828z781",
        },
        "us2012_2042": {
            "meta": "https://markets.businessinsider.com/bonds/3_125-us-staatsanleihen-bond-2042-us912810qu51",
        },
        "us2013_2043": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201343-bond-2043-us912810qz49",
        },
        "us2015_2045": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201545-bond-2045-us912810rn00",
        },
        "us2020_2030": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202030-bond-2030-us91282cae12",
        },
        "us2022_2025": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202225-bond-2025-us91282cfk27",
        },
        "us2023_2030": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202330-bond-2030-us91282cgs44",
        },
        "us2023_2026": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202326-bond-2026-us91282chh79",
        },
        "us2023_2028": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_202328-bond-2028-us91282chq78",
        },
        "us2019_2026": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201926-bond-2026-us912828yg91",
        },
        "us2018_2025": {
            "meta": "https://markets.businessinsider.com/bonds/united_states_of_americadl-notes_201825-bond-2025-us9128284f40",
        },
    },
    "corp_bonds_kr": {
        "EIBK2016_2027": {
            "meta": "https://markets.businessinsider.com/bonds/export-import_bk_of_korea-_thead-medium-term_notes_201627-bond-2027-au3cb0241248",
        },
        "SHBK2018_2028": {
            "meta": "https://markets.businessinsider.com/bonds/shinhan_bank_co-bond-2028-au3cb0256113",
        },
        "KNOL2020_2027": {
            "meta": "https://markets.businessinsider.com/bonds/korea_national_oil_corpsf-medium-term_notes_202027-bond-2027-ch0554992070",
        },
        "KWRS2023_2025": {
            "meta": "https://markets.businessinsider.com/bonds/korea_water_resources_corpsf-medium-term_notes_202325-bond-2025-ch1271360633",
        },
        "KELP1997_2027": {
            "meta": "https://markets.businessinsider.com/bonds/7_000-korea-electric-power-bond-2027-us500631ah98",
        },
        "KDVB2020_2026": {
            "meta": "https://markets.businessinsider.com/bonds/korea_development_bank-_thedl-notes_202026-bond-2026-us500630de57",
        },
        "KEXP2015_2027": {
            "meta": "https://markets.businessinsider.com/bonds/korea_expressway_corpdl-medium-term_notes_201527-bond-2027-xs1203861403",
        },
    },
    "corp_bonds_us": {
        "APPL2016_2026": {
            "meta": "https://markets.businessinsider.com/bonds/apple_incad-notes_201626-bond-2026-au3cb0237881",
        },
        "NYLF2019_2027": {
            "meta": "https://markets.businessinsider.com/bonds/new_york_life_global_fundingsf-medium-term_notes_201927-bond-2027-ch0471297959",
        },
        "JOHN1999_2029": {
            "meta": "https://markets.businessinsider.com/bonds/6_950-johnson-johnson-bond-2029-us478160aj37",
        },
        "MSCP2016_2026": {
            "meta": "https://markets.businessinsider.com/bonds/microsoft_corpdl-notes_201616-26-bond-2026-us594918br43",
        },
        "TNVA1995_2025": {
            "meta": "https://markets.businessinsider.com/bonds/6_750-tennessee-valley-authority-bond-2025-us880591cj98",
        },
    },
}


def get_meta_data(category, bond_name, **ctxt):
    # Bonds meta data crawling
    # TODO: ✅ Would it be better to do this on a separate DAG?
    # TODO: I should try the Soup on the industry code DAG?! 🤨
    res = requests.get(META[category][bond_name]["meta"])
    time.sleep(3)
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table")  # there is only one table

    data = {}
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) == 2:
            header = cols[0].text.strip()
            content = cols[1].text.strip()
            data[header] = data.get(header, content)

    ds_year, ds_month, ds_day = "{{ ds }}"[:4], "{{ ds }}"[5:7], "{{ ds }}"[8:10]
    upload = S3CreateObjectOperator(
        task_id=f"upload_{bond_name}",
        aws_conn_id="aws_conn_id",
        s3_bucket=S3_BUCKET,
        s3_key=f"bronze/{category}/kind={bond_name}/date={ds_year}-{ds_month}-{ds_day}/{category}_{bond_name}_meta_{ds_year}-{ds_month}.json",
        data=json.dumps(data),
        replace=True,
    )
    upload.execute(context=ctxt)

    time.sleep(3)

    return "puff"


with DAG(
    dag_id="brz_govt_bonds_meta_month",
    start_date=START_DATE,
    schedule_interval="0 0 1 * *",
    catchup=False,
    default_args={
        "retries": 0,
        "trigger_rule": "all_success",
        "owner": "dee",
    },
    max_active_tasks=1,
    tags=["bronze"],
    description="All Bonds Metadata, Govt And Corp",
) as dag:
    # Dynamically generate crawling tasks
    with TaskGroup(group_id="crawler_group") as meta_data_crawler_group:
        # Put prev_task right below category loop to parallelize
        prev_task = None
        for category in META:
            for bond_name in META[category]:
                curr_task = PythonOperator(
                    task_id=f"{category}_{bond_name}",
                    python_callable=get_meta_data,
                    op_args=[category, bond_name],
                )
                if prev_task:
                    prev_task >> curr_task
                prev_task = curr_task

    meta_data_crawler_group
