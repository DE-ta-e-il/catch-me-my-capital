import pandas as pd
import requests


def fetch_coin_data(symbols, coin_tmp_file_path, **kwargs):
    """
    Binance API를 통해 코인 데이터를 수집하는 함수
    """

    # Binance에서 유닉스 timestamp만 지원하기 때문에 변환 필요
    unix_timestamp = int(kwargs["logical_date"].timestamp() * 1000)

    # 데이터를 저장할 리스트
    all_data = []

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
            # 데이터 프레임으로 변환
            df = pd.DataFrame(
                data,
                columns=[
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
                ],
            )
            df["Symbol"] = symbol
            df["Name"] = name
            all_data.append(df)
        else:
            raise Exception(
                f"Failed to fetch data for {symbol}: {response.status_code}"
            )

    # 모든 데이터를 하나의 데이터 프레임으로 통합
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
    else:
        raise Exception("No data fetched.")

    # CSV 포맷으로 저장
    combined_data.to_csv(coin_tmp_file_path, index=False)
