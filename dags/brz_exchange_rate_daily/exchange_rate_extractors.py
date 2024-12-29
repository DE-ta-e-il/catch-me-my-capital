from datetime import timedelta

import pandas as pd
import yfinance as yf


def fetch_exchange_rates(currency_pairs, exchange_rate_tmp_file_path, **kwargs):
    """
    환율 데이터 수집 함수
    """

    # 데이터 수집
    data = yf.download(
        currency_pairs,
        period="1d",
        start=kwargs["logical_date"],
        end=kwargs["logical_date"] + timedelta(days=1),
    )

    # 종가 데이터 추출
    if isinstance(data.columns, pd.MultiIndex):
        close_data = data["Close"]
    else:
        close_data = data[["Close"]]

    # 날짜를 열로 추가
    close_data = close_data.reset_index()
    # Date를 파티션 키로 사용할 경우 충돌을 피하기 위해 칼럼 이름 변경
    close_data.rename(columns={"Date": "RecordDate"}, inplace=True)

    # CSV 포맷으로 저장
    close_data.to_csv(exchange_rate_tmp_file_path, index=False)
