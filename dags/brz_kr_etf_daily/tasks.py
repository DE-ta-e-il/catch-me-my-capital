import json
from datetime import datetime

import requests
from airflow.models import Variable
from common.s3_utils import upload_string_to_s3


def generate_json_s3_key(today_dash: str) -> str:
    return f"bronze/kr_etf/ymd={today_dash}/data.json"


def verify_market_open(**kwargs):
    calendar_result = kwargs["ti"].xcom_pull(
        key="return_value", task_ids="get_workday_info"
    )

    records = calendar_result["Records"][0]  # 첫 번째 레코드 (하나만 있음)

    today_is_holiday = records[1]["booleanValue"]
    previous_working_day = records[2]["stringValue"]  # yyyy-mm-dd

    if today_is_holiday:
        return False
    else:
        kwargs["ti"].xcom_push(key="previous_working_day", value=previous_working_day)
        return True


def fetch_etf_from_krx_api_to_s3(**kwargs):
    target_date = kwargs["ti"].xcom_pull(
        key="previous_working_day", task_ids="verify_market_open"
    )
    date_obj = datetime.strptime(target_date, "%Y-%m-%d")
    target_date_nodash = date_obj.strftime("%Y%m%d")

    url = "http://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService/getETFPriceInfo"
    params = {
        "serviceKey": Variable.get("data_kr_service_key"),
        "numOfRows": 1000,
        "pageNo": 1,
        "resultType": "json",
        "basDt": target_date_nodash,
    }

    all_items = []

    while True:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(
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

    data_str = json.dumps({"items": all_items}, ensure_ascii=False)
    s3_key = generate_json_s3_key(target_date)

    upload_string_to_s3(data_str, s3_key)
