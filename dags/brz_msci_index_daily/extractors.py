import calendar
import json
from datetime import timedelta

import requests
from common import upload_file_to_s3


def fetch_msci_indices_data(
    msci_url_info, msci_index_tmp_file_path, msci_index_data_s3_key, **kwargs
):
    """
    MSCI World Index, MSCI Emerging Market Index 데이터를 개별 파일로 수집
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

    for index_name, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            record = data[0]
            record["Index_Name"] = index_name
            record["RecordDate"] = record.pop("Date", None)

            # 각 인덱스별 파일 경로 생성
            # msci_index_data_s3_key가 's3://bucket/bronze/msci_index/ymd=2024-02-14/data.json' 형식이라고 가정
            base_path = msci_index_data_s3_key.rsplit("/", 1)[0]
            file_name = f"msci_{index_name}.json"
            index_s3_key = f"{base_path}/{file_name}"

            # 임시 파일 생성
            tmp_file = msci_index_tmp_file_path.replace(".json", f"_{index_name}.json")
            with open(tmp_file, "w") as jsonfile:
                json.dump([record], jsonfile, indent=4)  # 단일 레코드를 리스트로 저장

            # S3에 업로드
            upload_file_to_s3(
                key=index_s3_key,
                file_path=tmp_file,
            )
        else:
            raise Exception(
                f"Failed to fetch data for {index_name}: {response.status_code}"
            )
