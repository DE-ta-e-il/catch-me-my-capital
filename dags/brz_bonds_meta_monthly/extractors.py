import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup

from brz_bonds_meta_monthly.constants import ProvidersParam
from brz_bonds_meta_monthly.uploaders import upload_bonds_metadata_to_s3


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
def get_metadata(category, bond_name, **ctxt):
    # Fetch the urls file
    s3 = S3Hook(aws_conn_id="aws_conn_id")
    file = s3.read_key(
        key="data/urls_bonds.json", bucket_name=ProvidersParam.S3_BUCKET.value
    )
    urls_dict = json.loads(file)

    # Bonds meta data crawling
    res = requests.get(urls_dict[category][bond_name]["meta"])
    time.sleep(3)
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table")  # there is only one table tag

    data = {}
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) == 2:
            header = cols[0].text.strip()
            content = cols[1].text.strip()
            if content:
                data[header] = data.get(header, content)
                data["name"] = bond_name

    date = datetime.strptime(ctxt["ds"], "%Y-%m-%d").strftime("%Y-%m-%d")
    key = f"bronze/{category}/ymd={date}/{category}_{bond_name}_meta_{date[:7]}.json"
    upload_bonds_metadata_to_s3(data, key)
