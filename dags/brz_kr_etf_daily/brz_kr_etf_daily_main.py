import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from common.s3_utils import upload_string_to_s3

from brz_kr_etf_daily.brz_kr_etf_daily_utils import (
    generate_json_s3_key,
    is_kr_market_open_today,
)


def verify_market_open(ds_nodash):
    date = datetime.strptime(ds_nodash, "%Y%m%d")
    return is_kr_market_open_today(date)


def fetch_etf_from_krx_api_to_s3(ds_nodash, ds):
    url = "http://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService/getETFPriceInfo"
    params = {
        "serviceKey": Variable.get("data_kr_service_key"),
        "numOfRows": 1000,
        "pageNo": 1,
        "resultType": "json",
        "basDt": ds_nodash,
    }

    all_items = []

    while True:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise AirflowFailException(
                f"Failed to fetch API: {response.status_code}, {response.text}"
            )

        data = response.json().get("response", {}).get("body", {})

        total_count = int(data.get("totalCount"))
        current_page = int(data.get("pageNo"))

        if total_count == 0:
            raise Exception(f"No data available: 'totalCount' is 0. Full data: {data}")

        items = data.get("items", {}).get("item", [])
        all_items.extend(items)

        if len(all_items) >= total_count:
            break

        params["pageNo"] = current_page + 1

    data_str = json.dumps(all_items)
    s3_key = generate_json_s3_key(ds)

    upload_string_to_s3(data_str, s3_key)


default_args = {
    "owner": "j-eum",  # TODO: 공통 ENUM적용 예정
    "retries": 5,
    "retry_delay": timedelta(hours=1),
}

with DAG(
    dag_id="brz_kr_etf_daily",
    default_args=default_args,
    description="한국거래소 ETF 종목별 시세",
    tags=["bronze", "ETF", "daily", "weekday"],
    schedule="0 0 * * 1-5",
    start_date=datetime(2020, 1, 1),
    catchup=False,
) as dag:
    verify_market_open = ShortCircuitOperator(
        task_id="verify_market_open", python_callable=verify_market_open
    )

    fetch_etf_from_krx_api_to_s3 = PythonOperator(
        task_id="fetch_etf_krx_api",
        python_callable=fetch_etf_from_krx_api_to_s3,
    )

    verify_market_open >> fetch_etf_from_krx_api_to_s3
