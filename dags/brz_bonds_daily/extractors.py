import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_bonds_daily.constants import FIRST_RUN, S3_BUCKET, START_DATE
from brz_bonds_daily.uploaders import upload_bonds_to_s3


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

            for dt, daily_list in gbd.items():
                key = f"bronze/{bond_kind}/kind={target}/date={dt}/{target}_{dt}.json"
                upload_bonds_to_s3(daily_list, key)

        # If it's not the first rodeo, run normal operation 🔫🤠🐂
        else:
            key = f"bronze/{bond_kind}/kind={target}/date={date}/{target}_{date}.json"
            upload_bonds_to_s3(response.json(), key)
