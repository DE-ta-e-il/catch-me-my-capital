import json

import requests
from common.s3_utils import upload_file_to_s3


def fetch_news_data(temp_file_path, api_key, news_data_s3_key, **kwargs):
    """
    뉴욕타임즈 기사 데이터를 수집하는 함수 (1달 단위, 1주마다 Full Refresh)
    """
    year = kwargs["logical_date"].year
    month = kwargs["logical_date"].month
    url = (
        f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
    )

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open(temp_file_path, "w") as f:
            json.dump(data, f)
    else:
        raise Exception(f"Failed to fetch news data: {response.status_code}")

    upload_file_to_s3(
        s3_key=news_data_s3_key,
        file_path=temp_file_path,
    )
