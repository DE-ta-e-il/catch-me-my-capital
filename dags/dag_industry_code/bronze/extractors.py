import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup
from constants import FIRST_RUN, S3_BUCKET, START_DATE
from uploaders import (
    upload_bonds_metadata_to_s3,
    upload_bonds_to_s3,
)


# Business Insider API endpoint url generator
def generate_urls(**ctxt):
    # Hooks, too, must be wrapped in here...
    # Get the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(key="data/urls_bonds.json", bucket_name=S3_BUCKET)
    urls_dict = json.loads(file)

    # Date range for the url query strings
    ds = ctxt["ds"]
    d_range = (
        (datetime(2015, 1, 1), START_DATE)
        if FIRST_RUN
        else (
            (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            datetime.strptime(ds, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
        )
    )
    # NOTE: Maximum Moody's rating for KR corp bonds are Aa2. Data points : once a day.
    # no xcom no ☠
    full_urls = {
        bond_kind: {
            bond: f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData={urls_dict[bond_kind][bond]['chart']}&from={d_range[0].strftime('%Y%m%d')}&to={d_range[1].strftime('%Y%m%d')}"
            for bond in urls_dict[bond_kind]
        }
        for bond_kind in urls_dict
    }

    with open("/tmp/full_urls_bonds.json", "w") as file:
        json.dump(full_urls, file)


# A dynamic task template for fetching bond data from Business insider API
def get_bond_data(bond_kind, **ctxt):
    ds = ctxt["ds"]
    date = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%m-%d")

    # Fetch urls
    with open("/tmp/full_urls_bonds.json", "r") as file:
        full_urls = json.load(file)

    # Fetching the ranged data
    for target in full_urls[bond_kind]:
        response = requests.get(full_urls[bond_kind][target])
        time.sleep(3)

        # If this is the first run,
        # unravel 10-years-worth of data
        # and save them as smaller partitions
        if FIRST_RUN:
            # gbd: Short for grouped-by-day
            gbd = defaultdict(list)
            for rec in response.json():
                date = rec["Date"]
                gbd[date[:10]].append(rec)

            if len(gbd) == 0:
                raise Exception("Nothing was fetched")

            # NOTE: Wonder if this is practical?
            for date, daily_list in gbd.items():
                upload_bonds_to_s3(date, target, daily_list)

        # If it's not the first rodeo, run normal operation 🔫🤠🐂
        else:
            upload_bonds_to_s3(date, target, response.json())


# For KRX APIs' industry codes
def fetch_industry_codes(market, referer, mktId, **ctxt):
    """
    For KRX KOSPI and KOSDAQ industry codes but it can be expanded(NOT compatible with GICS crawling).
    """
    date = ctxt["ds"]
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


# Fetches urls data and returns category name and bond name
def get_categories():
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(key="data/urls_bonds.json", bucket_name=S3_BUCKET)
    res = json.loads(file)
    titles = {category: [bond_name for bond_name in res[category]] for category in res}
    return titles


# Get all bonds' metadata
def get_metadata(category, bond_name, **ctxt):
    # Fetch the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(key="data/urls_bonds.json", bucket_name=S3_BUCKET)
    urls_dict = json.loads(file)

    # Bonds meta data crawling
    # TODO: ✅ Would it be better to do this on a separate DAG?
    # TODO: ✅I should try the Soup on the industry code DAG?! 🤨
    res = requests.get(urls_dict[category][bond_name]["meta"])
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

    date = datetime.strptime(ctxt["ds"], "%Y-%m-%d").strftime("%Y-%m-%d")
    upload_bonds_metadata_to_s3(date, category, bond_name, data)
