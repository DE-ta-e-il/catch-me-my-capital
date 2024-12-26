import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)


def fetch_and_upload_coin_data(execution_date: datetime, **kwargs):
    """
    Binance API를 통해 코인 데이터를 수집하는 함수
    """
    # 수집할 코인 심볼과 이름
    symbols = {
        "BTCUSDT": "Bitcoin",
        "ETHUSDT": "Ethereum",
        "BNBUSDT": "BinanceCoin",
        "XRPUSDT": "Ripple",
        "ADAUSDT": "Cardano",
        "SOLUSDT": "Solana",
        "DOTUSDT": "Polkadot",
        "DOGEUSDT": "Dogecoin",
        "AVAXUSDT": "Avalanche",
        "SHIBUSDT": "ShibaInu",
        "MATICUSDT": "Polygon",
        "LTCUSDT": "Litecoin",
        "UNIUSDT": "Uniswap",
        "LINKUSDT": "Chainlink",
        "BCHUSDT": "BitcoinCash",
        "XLMUSDT": "Stellar",
        "ATOMUSDT": "Cosmos",
        "ALGOUSDT": "Algorand",
        "VETUSDT": "VeChain",
        "FILUSDT": "Filecoin",
        "USDCUSDT": "USDC",
        "PEPEUSDT": "PEPE",
        "XMRUSDT": "Monero",
        "ZECUSDT": "Zcash",
        "XEMUSDT": "NEM",
    }

    # Binance에서 유닉스 timestamp만 지원하기 때문에 변환 필요
    unix_timestamp = int(execution_date.timestamp() * 1000)

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
                    "Open time",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Close time",
                    "Quote asset volume",
                    "Number of trades",
                    "Taker buy base asset volume",
                    "Taker buy quote asset volume",
                    "Ignore",
                ],
            )
            df["Symbol"] = symbol
            df["Name"] = name
            all_data.append(df)
        else:
            print(f"Failed to fetch data for {symbol}: {response.status_code}")

    # 모든 데이터를 하나의 데이터 프레임으로 통합
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
    else:
        print("No data fetched.")
        return pd.DataFrame()

    # 임시 파일로 CSV 저장
    temp_file_path = f"/tmp/coin_data_{execution_date.strftime('%Y-%m-%d')}.parquet"
    combined_data.to_parquet(temp_file_path, index=False)

    # S3에 업로드
    s3_upload_task = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=temp_file_path,
        dest_bucket="team3-1-s3",
        dest_key=f"bronze/coin_data/date={execution_date.strftime('%Y-%m-%d')}/{execution_date.strftime('%Y-%m-%d')}_coin_data.parquet",
        replace=True,
        aws_conn_id="aws_conn_id",
    )

    s3_upload_task.execute(context=kwargs)

    # 임시 파일 삭제
    os.remove(temp_file_path)


default_args = {
    "owner": "JeongMinHyeok",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="brz_coin_daily",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_tasks=5,
    tags=["bronze", "daily"],
) as dag:
    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_coin_data",
        python_callable=fetch_and_upload_coin_data,
        provide_context=True,
    )

    fetch_and_upload_task
