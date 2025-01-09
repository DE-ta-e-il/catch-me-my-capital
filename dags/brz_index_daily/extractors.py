import json
import logging

import cloudscraper
from common.s3_utils import upload_file_to_s3

logger = logging.getLogger(__name__)


def fetch_index_data(symbols, index_tmp_file_path, index_data_s3_key, **kwargs):
    modified_response = []

    for symbol, code in symbols.items():
        # cloudscraper 객체 생성
        scraper = cloudscraper.create_scraper()
        # URL 설정
        url = f"https://api.investing.com/api/financialdata/historical/{code}"

        # 요청 파라미터 설정
        params = {
            "start-date": kwargs["logical_date"].strftime("%Y-%m-%d"),
            "end-date": kwargs["logical_date"].strftime("%Y-%m-%d"),
            "time-frame": "Daily",
            "add-missing-rows": "false",
        }

        # 필수 헤더 설정
        headers = {
            "domain-id": "www",
        }

        # GET 요청 보내기
        response = scraper.get(url, headers=headers, params=params)
        logger.info(f"response.status_code: {response.status_code}")

        # 에러 발생 시 Exception 발생
        if response.status_code != 200:
            raise Exception(f"Error fetching data for {symbol}: {response.text}")

        response_json = response.json()
        # 휴장일에는 데이터가 None으로 반환됨 - 각 시장마다 휴장일이 다르므로 이렇게 처리
        if response_json["data"] is not None:
            data = response_json["data"][0]
            data["index_name"] = symbol
            modified_response.append(data)

    # json 파일로 저장
    with open(index_tmp_file_path, "w") as f:
        json.dump(modified_response, f, indent=4)

    # s3에 업로드
    upload_file_to_s3(
        key=index_data_s3_key,
        file_path=index_tmp_file_path,
    )
