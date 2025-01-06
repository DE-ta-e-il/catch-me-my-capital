import json
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from airflow.models import Variable
from common.s3_utils import upload_string_to_s3


def is_kr_market_open_today(today: datetime) -> bool:
    """
    TEMPORARY: This function is currently used as a placeholder for future functionality.
    It will be removed once the new feature is implemented.

    Note: Remove after [date or milestone].
    """
    year = today.strftime("%Y")
    month = today.strftime("%m")
    today_nodash = today.strftime("%Y%m%d")

    url = (
        "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
    )
    params = {
        "solYear": year,
        "solMonth": month,
        "serviceKey": Variable.get("data_kr_service_key"),
    }

    response = requests.get(url, params=params)
    root = ET.fromstring(response.text)
    # 모든 <locdate> 태그의 값 추출
    locdates = [item.text for item in root.findall(".//locdate")]

    if int(month) == 12:
        last_weekday = datetime(today.year, 12, 31)

        while last_weekday.weekday() > 4:
            last_weekday -= datetime.timedelta(days=1)

        last_weekday_nodash = last_weekday.strftime("%Y%m%d")
        locdates.append(last_weekday_nodash)

    # today와 일치하는 값이 있는지 확인 -> 휴장일
    if today_nodash in locdates:
        return False
    else:
        return True


def generate_json_s3_key(today_dash: str) -> str:
    return f"bronze/kr_etf/ymd={today_dash}/data.json"


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

    data_str = json.dumps(items, ensure_ascii=False)
    s3_key = generate_json_s3_key(ds)

    upload_string_to_s3(data_str, s3_key)
