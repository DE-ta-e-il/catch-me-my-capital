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
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(
        key="data/urls_bonds.json", bucket_name=ProvidersParam.S3_BUCKET.value
    )
    res = json.loads(file)
    titles = {category: [bond_name for bond_name in res[category]] for category in res}
    return titles


# Get all bonds' metadata
def get_metadata(category, **ctxt):
    # Fetch the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(
        key="data/urls_bonds.json", bucket_name=ProvidersParam.S3_BUCKET.value
    )
    urls_dict = json.loads(file)

    # Bonds meta data crawling
    payload = []
    for bond_key in urls_dict[category]:
        res = requests.get(urls_dict[category][bond_key]["meta"])
        time.sleep(2)
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table")  # there is only one table tag

        parsed = {}
        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                header = cols[0].text.strip().replace(" ", "_").lower()
                content = cols[1].text.strip()
                if content:
                    parsed[header] = parsed.get(header, content)
                    parsed["name"] = bond_key

        payload.append(parsed)

    date = datetime.strptime(ctxt["ds"], "%Y-%m-%d").strftime("%Y-%m-%d")
    key = f"bronze/{category}_meta/ymd={date}/{category}_meta_{date[:7]}.json"
    upload_string_to_s3(json.dumps(payload, indent=4), key)
