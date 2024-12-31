import json
from datetime import datetime

import requests
from airflow.exceptions import AirflowFailException
from common.s3_utils import upload_string_to_s3


def fetch_krx_market_holiday_to_s3(ds_nodash: str) -> None:
    date = datetime.strptime(ds_nodash, "%Y%m%d")
    year = date.year

    url = "https://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx"
    data = {
        "search_bas_yy": year,
        "gridTp": "KRX",
        "pagePath": "/contents/MKD/01/0110/01100305/MKD01100305.jsp",
        "code": "HNEN5o2OSEUYbQsVG1wQVWBUSiNmmZQy8buCiZIRczY2J4tH9fWxwOAl3udbjZkMcrSVxmhQX0JyB60Rdlu2EIjgNo4zuoT2DC9AK+1W7IRPKOy0d+sLyWLiwKXMM19oK2+/NZEt3I5FBGlvD4ljaXxrUyY5PihJiCOHrQqBVwyIuHtCncOuvwwLYWlnntdn",  # pragma: allowlist-secret
        "pageFirstCall": "Y",
    }

    response = requests.post(url, data=data)

    if response.status_code != 200:
        raise AirflowFailException(
            f"Failed to fetch API: {response.status_code}, {response.text}"
        )

    data = response.json()
    items = data.get("block1")

    if not items:
        raise Exception(
            f"Data retrieval failed: 'output' is missing or empty. Full data: {data}"
        )

    data_str = json.dumps(items)
    s3_key = f"bronze/kr_market_holiday/year={year}/data.json"

    upload_string_to_s3(data_str, s3_key)
