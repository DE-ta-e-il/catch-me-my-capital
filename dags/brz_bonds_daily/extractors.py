import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from brz_bonds_daily.constants import FIRST_RUN, S3_BUCKET, START_DATE
from brz_bonds_daily.uploaders import upload_to_s3


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

    upload_to_s3(full_urls, "data/full_urls_bonds.json")


# A dynamic task template for fetching bond data from Business insider API
def get_bond_data(bond_category, **ctxt):
    ds = ctxt["ds"]
    date = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%m-%d")

    # Fetch urls
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(key="data/full_urls_bonds.json", bucket_name=S3_BUCKET)
    full_urls = json.loads(file)

    # Fetching the ranged data
    for bond_kind in full_urls[bond_category]:
        response = requests.get(full_urls[bond_category][bond_kind])
        time.sleep(3)

        # gbd: Short for grouped-by-day
        gbd = defaultdict(list)
        for rec in response.json():
            date = rec["Date"]
            price = rec.pop("Close")
            rec.update(
                {
                    "Price": price,
                    "name": bond_kind,
                    "matures_in": int(bond_kind[-4:]) - int(bond_kind[-9:-5]),
                }
            )
            gbd[date[:10]].append(rec)

        if len(gbd) == 0:
            raise Exception("Nothing was fetched")

        for dt, daily_list in gbd.items():
            key = f"bronze/{bond_category}/ymd={dt}/{bond_kind}_{dt}.json"
            upload_to_s3(daily_list, key)
