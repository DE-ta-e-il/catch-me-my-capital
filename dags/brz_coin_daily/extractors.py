import csv

import requests
from common import upload_file_to_s3


def fetch_coin_data(symbols, coin_tmp_file_path, coin_data_s3_key, **kwargs):
    """
    Binance API를 통해 코인 데이터를 수집하는 함수
    """

    # Binance에서 유닉스 timestamp만 지원하기 때문에 변환 필요
    unix_timestamp = int(kwargs["logical_date"].timestamp() * 1000)

    # 데이터를 저장할 리스트 (칼럼들을 먼저 삽입해놓음)
    all_data = [
        [
            "Open_time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close_time",
            "Quote_asset_volume",
            "Number_of_trades",
            "Taker_buy_base_asset_volume",
            "Taker_buy_quote_asset_volume",
            "Ignore",
            "Symbol",
            "Name",
        ]
    ]

    for symbol, name in symbols.items():
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1d",
            "startTime": unix_timestamp,
            "endTime": unix_timestamp,
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()

            for row in data:
                row.extend([symbol, name])
            all_data.extend(data)
        else:
            raise Exception(
                f"Failed to fetch data for {symbol}: {response.status_code}"
            )

    # CSV 포맷으로 저장
    if all_data:
        with open(coin_tmp_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerows(all_data)
    else:
        raise Exception("No data fetched.")

    # S3에 업로드
    upload_file_to_s3(
        key=coin_data_s3_key,
        file_path=coin_tmp_file_path,
    )
