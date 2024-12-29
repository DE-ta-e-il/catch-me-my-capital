import calendar
import csv
from datetime import timedelta

import requests


def fetch_msci_indices_data(msci_url_info, msci_index_tmp_file_path, **kwargs):
    """
    MSCI World Index, MSCI Emerging Market Index 데이터 수집 함수
    """

    month = calendar.month_abbr[kwargs["logical_date"].month]
    day = (kwargs["logical_date"] - timedelta(days=1)).strftime("%d")
    year = kwargs["logical_date"].year

    urls = {
        "MSCI_World": msci_url_info["MSCI_WORLD_URL"].format(
            month=month, day=day, year=year
        ),
        "MSCI_Emerging": msci_url_info["MSCI_EMERGING_URL"].format(
            month=month, day=day, year=year
        ),
    }

    all_data = []
    for index_name, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data[0]["Index"] = index_name
            # Date를 파티션 키로 사용할 경우 충돌을 피하기 위해 칼럼 이름 변경
            data[0]["RecordDate"] = data[0].pop("Date")
            all_data.extend(data)
        else:
            raise Exception(
                f"Failed to fetch data for {index_name}: {response.status_code}"
            )

    # CSV 포맷으로 저장
    with open(msci_index_tmp_file_path, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_data[0].keys(), delimiter=",")
        writer.writeheader()
        writer.writerows(all_data)
