# 통화 리스트
CURRENCY_PAIRS = [
    "USDKRW=X",  # 미국 달러/한국 원
    "EURKRW=X",  # 유럽 연합 유로/한국 원
    "GBPKRW=X",  # 영국 파운드/한국 원
    "AUDKRW=X",  # 호주 달러/한국 원
    "JPYKRW=X",  # 일본 엔/한국 원
    "INRKRW=X",  # 인도 루피/한국 원
    "ZARKRW=X",  # 남아프리카 공화국 랜드/한국 원
    "EURUSD=X",  # 유럽 연합 유로/미국 달러
    "AUDUSD=X",  # 호주 달러/미국 달러
    "JPYUSD=X",  # 일본 엔/미국 달러
    "CNYUSD=X",  # 중국 위안/미국 달러
    "GBPUSD=X",  # 영국 파운드/미국 달러
]

EXCHANGE_RATE_TMP_FILE_PATH = "/tmp/exchange_rates_{{ ds }}.csv"
EXCHANGE_RATE_DATA_S3_KEY = (
    "bronze/exchange_rate/ymd={{ ds }}/{{ ds }}_exchange_rates.csv"
)
