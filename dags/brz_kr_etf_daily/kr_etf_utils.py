import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from airflow.models import Variable


def is_kr_market_open_today(today: datetime) -> bool:
    year = today.year
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
        last_weekday = datetime(year, 12, 31)

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
    return f"bronze/kr_etf/date={today_dash}/data.json"
