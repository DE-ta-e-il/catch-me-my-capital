import json
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from common.s3_utils import upload_string_to_s3

from brz_kr_etf_daily.brz_kr_etf_daily_utils import (
    generate_json_s3_key,
    is_kr_market_open_today,
)


def verify_market_open(ds_nodash):
    date = datetime.strptime(ds_nodash, "%Y%m%d")
    return is_kr_market_open_today(date)


def fetch_etf_from_krx_web_to_s3(ds_nodash, ds):
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT04301",
        "locale": "ko_KR",
        "trdDd": ds_nodash,
        "share": 1,
        "money": 1,
        "csvxls_isNo": False,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.132 Safari/537.36",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020203",
    }

    response = requests.post(url, data=data, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch API: {response.status_code}, {response.text}")

    data = response.json()
    items = data.get("output")

    if not items:
        raise Exception(
            f"Data retrieval failed: 'output' is missing or empty. Full data: {data}"
        )

    data_str = json.dumps(items)
    s3_key = generate_json_s3_key(ds)

    upload_string_to_s3(data_str, s3_key)


default_args = {
    "owner": "j-eum",  # TODO: 공통 ENUM적용 예정
}

with DAG(
    dag_id="brz_kr_etf_daily_deprecated",
    default_args=default_args,
    description="한국거래소 ETF 종목별 시세",
    tags=["bronze", "ETF", "daily", "weekday"],
    schedule="0 0 * * 1-5",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2019, 12, 31),
    catchup=False,
) as dag:
    verify_market_open = ShortCircuitOperator(
        task_id="verify_market_open", python_callable=verify_market_open
    )

    fetch_etf_from_krx_web_to_s3 = PythonOperator(
        task_id="fetch_etf_krx_web",
        python_callable=fetch_etf_from_krx_web_to_s3,
    )

    verify_market_open >> fetch_etf_from_krx_web_to_s3
