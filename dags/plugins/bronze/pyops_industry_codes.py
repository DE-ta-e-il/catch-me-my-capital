import json
import time
from datetime import datetime

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup

import plugins.bronze.constants as C


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

    time.sleep(10)

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

    with open(f"/tmp/{market}_industry_codes.json", "w") as file:
        json.dump(new_items, file)


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

    date = ctxt["ds"]
    date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")

    with open("/tmp/gics_industry_codes.json", "w") as file:
        json.dump([sectors, industry_group, industry, sub_industry], file)


# Task 3
def upload_to_s3(target, **ctxt):
    with open(f"/tmp/{target}_industry_codes.json", "r") as file:
        data = json.load(file)
        s3 = S3Hook(aws_conn_id="aws_conn_id")
        s3.load_string(
            string_data=json.dumps(data),
            bucket_name=C.S3_BUCKET,
            key=f"bronze/industry_code/date={ctxt['ds']}/{target}_codes_{ctxt['ds']}.json",
            replace=True,
        )
