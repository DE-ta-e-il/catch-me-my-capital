import json

import requests
from common.s3_utils import upload_file_to_s3


def fetch_news_data(temp_file_path, api_key, news_data_s3_key, **kwargs):
    """
    뉴욕타임즈 기사 데이터를 수집하고, 주요 데이터만 추출하여 S3에 저장합니다.
    """
    year = kwargs["logical_date"].year
    month = kwargs["logical_date"].month

    # 뉴욕타임즈 API 호출
    url = (
        f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
    )
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # 주요 데이터만 추출
        docs = data.get("response", {}).get("docs", [])
        processed_data = []

        for doc in docs:
            # 필요한 필드만 선택
            processed_doc = {
                "abstract": doc.get("abstract"),
                "web_url": doc.get("web_url"),
                "headline": doc.get("headline", {}).get("main"),
                "pub_date": doc.get("pub_date"),
                "section_name": doc.get("section_name"),
                "byline": doc.get("byline", {}).get("original"),
                "word_count": doc.get("word_count"),
                # keywords 필드에서 value 값만 리스트로 추출
                "keywords": [
                    keyword.get("value") for keyword in doc.get("keywords", [])
                ],
            }
            processed_data.append(processed_doc)

        # JSON 파일 저장
        with open(temp_file_path, "w") as f:
            json.dump(processed_data, f, indent=4)

        # S3에 업로드
        upload_file_to_s3(
            key=news_data_s3_key,
            file_path=temp_file_path,
        )
    else:
        raise Exception(f"Failed to fetch news data: {response.status_code}")
