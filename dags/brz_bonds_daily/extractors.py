import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common.s3_utils import upload_string_to_s3

from brz_bonds_daily.constants import AirflowParam, ProvidersParam


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

    # NOTE: Maximum Moody's rating for KR corp bonds are Aa2. Data points : once a day.
    # NOTE: Now fetches all the data from start date to execution date
    # no xcom no ☠
    full_urls = {
        bond_kind: {
            bond: f"https://markets.businessinsider.com/Ajax/Chart_GetChartData?instrumentType=Bond&tkData={urls_dict[bond_kind][bond]['chart']}&from={AirflowParam.START_DATE.value.strftime('%Y%m%d')}&to={dt.strftime('%Y%m%d')}"
            for bond in urls_dict[bond_kind]
        }
        for bond_kind in urls_dict
    }
    upload_string_to_s3(json.dumps(full_urls, indent=4), "data/full_urls_bonds.json")


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
        time.sleep(2)
        response = requests.get(full_urls[bond_category][bond_kind])
        time.sleep(5)

        result = response.json()
        # API soft-fails to []
        # add empty records for those that are outside issuance-maturity range
        if not result:
            gbd[date].append(
                {
                    "bond_key": bond_kind,
                    "matures_in": 0,
                    "Close": 0,
                    "Open": 0,
                    "High": 0,
                    "Low": 0,
                    "Volume": 0,
                    "Estimate": 0,
                    "Date": date,
                }
            )
        else:
            for rec in result:
                if isinstance(rec, dict):
                    # Add necessary info
                    rec.update(
                        {
                            "bond_key": bond_kind,
                            "matures_in": int(bond_kind[-4:]) - int(bond_kind[-9:-5]),
                        }
                    )

                    gbd[rec["Date"][:10]].append(rec)

    if len(gbd) == 0:
        raise Exception(f"{bond_category} for {date} is empty.")

    for dt, daily_list in gbd.items():
        key = f"bronze/{bond_category}/ymd={dt}/{bond_category}_{dt}.json"
        payload = json.dumps(daily_list, indent=4)
        upload_string_to_s3(payload, key)
