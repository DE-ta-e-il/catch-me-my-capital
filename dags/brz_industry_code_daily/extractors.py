import json
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from common.s3_utils import upload_string_to_s3


# For KRX APIs' industry codes
def fetch_industry_codes(market, referer, mktId, **ctxt):
    """
    For KRX KOSPI and KOSDAQ industry codes but it can be expanded(NOT compatible with GICS crawling).
    """
    # date validation
    date = ctxt["ds"]
    date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")

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
                "trdDd": date,
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
                    "market": market,
                }
            )

    if len(new_items) == 0:
        raise Exception("NOPE NOT GETTING ANY")

    key = f"bronze/industry_code/krx_codes/ymd={date}/krx_codes_{date}.json"
    stringified = json.dumps(new_items, indent=4, ensure_ascii=False)
    upload_string_to_s3(stringified, key)


# For crawling GICS
def crawl_industry_codes(**ctxt):
    """
    This crawls for GICS industry codes but it could be expanded.
    It takes no parameter other than the airflow contexts.
    """
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
    sectors, industry_group, industry, sub_industry = [], [], [], []
    for i, r in enumerate(rows):
        if i % 2 == 0:  # Even indices indicate the name of the previous odd indices
            target = r.text.strip()
            name = rows[i + 1].text.strip()
            if len(target) == 2:
                sectors.append({"code": target, "name": name})
            elif len(target) == 4:
                industry_group.append({"code": target, "name": name})
            elif len(target) == 6:
                industry.append({"code": target, "name": name})
            else:
                sub_industry.append({"code": target, "name": name})

    date = ctxt["ds"]
    date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")

    for category, payload in {
        "sector": sectors,
        "industry_group": industry_group,
        "industry": industry,
        "sub_industry": sub_industry,
    }.items():
        key = f"bronze/industry_code/gics_codes/ymd={date}/gics_{category}_codes_{date}.json"
        stringified = json.dumps(payload, indent=4)
        upload_string_to_s3(stringified, key)
