import json
import time
from datetime import datetime

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup
from common.s3_utils import upload_string_to_s3

from brz_bonds_meta_monthly.constants import ProvidersParam


# Fetches urls data and returns category name and bond name
def get_categories():
    try:
        s3 = S3Hook(aws_conn_id="aws_conn_id")
        file = s3.read_key(
            key="data/urls_bonds.json", bucket_name=ProvidersParam.S3_BUCKET.value
        )
    except Exception as e:
        raise Exception(e)

    return json.loads(file)


def convert_date(us_dt_string):
    return datetime.strptime(us_dt_string, "%m/%d/%Y").strftime("%Y-%m-%d")


# Get all bonds' metadata
def get_metadata(**ctxt):
    # Fetch the urls file
    urls_dict = get_categories()

    # Bonds meta data crawling
    payload = []
    for category in urls_dict:
        for bond_key in urls_dict[category]:
            res = requests.get(urls_dict[category][bond_key]["meta"])
            time.sleep(2)
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table")  # there is only one table tag

            parsed = {}
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) == 2:
                    header = (
                        cols[0]
                        .text.strip()
                        .replace(".", "")
                        .replace(" ", "_")
                        .replace("?", "")
                        .lower()
                    )
                    content = cols[1].text.strip()
                    if content:
                        if header == "issue_volume":
                            content = int(content.replace(",", ""))
                        if header in [
                            "coupon",
                            "issue_price",
                            "denomination",
                            "no_of_payments_per_year",
                        ]:
                            content = float(content.replace("%", ""))
                        if header[-4:] == "date":
                            content = convert_date(content)
                        if header == "floater":
                            content = True if content == "Yes" else False
                        parsed[header] = parsed.get(header, content)
            parsed.update({"bond_type": category[:4], "bond_key": bond_key})
            payload.append(parsed)

    date = datetime.strptime(ctxt["ds"], "%Y-%m-%d").strftime("%Y-%m-%d")
    key = f"bronze/bonds_meta/ymd={date}/bonds_meta_{date[:7]}.json"
    upload_string_to_s3(json.dumps(payload, indent=4), key)
