import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_bonds_daily.constants import AirflowParam, ProvidersParam
from brz_bonds_daily.uploaders import upload_to_s3


# Business Insider API endpoint url generator
def generate_urls(**ctxt):
    # Hooks, too, must be wrapped in here...
    # Get the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(
        key="data/urls_bonds.json", bucket_name=ProvidersParam.S3_BUCKET.value
    )
    urls_dict = json.loads(file)

    # Date range for the url query strings
    dt = datetime.strptime(ctxt["ds"], "%Y-%m-%d")
    d_range = (
        (datetime(2015, 1, 1), AirflowParam.START_DATE.value)
        if AirflowParam.FIRST_RUN.value
        else (
            (dt - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0),
            dt.replace(hour=0, minute=0, second=0, microsecond=0),
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
    upload_to_s3(json.dumps(full_urls, indent=4), "data/full_urls_bonds.json")


# A dynamic task template for fetching bond data from Business insider API
def get_bond_data(bond_category, **ctxt):
    date = datetime.strptime(ctxt["ds"], "%Y-%m-%d").strftime("%Y-%m-%d")

    # Fetch urls
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(
        key="data/full_urls_bonds.json", bucket_name=ProvidersParam.S3_BUCKET.value
    )
    full_urls = json.loads(file)

    # Fetching the ranged data
    # gbd: Short for grouped-by-day
    gbd = defaultdict(list)
    for bond_kind in full_urls[bond_category]:
        response = requests.get(full_urls[bond_category][bond_kind])
        time.sleep(3)

        for rec in response.json():
            if isinstance(rec, dict):
                date = rec["Date"]
                # Add necessary info
                rec.update({"bond_key": bond_kind})
                gbd[date[:10]].append(rec)

    if len(gbd) == 0:
        raise Exception("Nothing was fetched")

    for dt, daily_list in gbd.items():
        key = f"bronze/{bond_category}/ymd={dt}/{bond_category}_{dt}.json"
        payload = json.dumps(daily_list, indent=4)
        upload_to_s3(payload, key)
