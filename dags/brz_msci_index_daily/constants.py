# MSCI index 데이터 수집 URL
MSCI_URL_INFO = {
    "MSCI_WORLD_URL": "https://markets.businessinsider.com/ajax/Valor_HistoricPriceList/169323/{month}.%20{day}%20{year}_.{month}.%20{day}%20{year}/MSCB",
    "MSCI_EMERGING_URL": "https://markets.businessinsider.com/ajax/Valor_HistoricPriceList/729220/{month}.%20{day}%20{year}_{month}.%20{day}%20{year}/MST",
}
# MSCI index 데이터 수집 임시 파일 경로
MSCI_INDEX_TMP_FILE_PATH = "/tmp/msci_index_{{ ds }}.json"
# MSCI index 데이터 수집 데이터 파일 S3 키
MSCI_INDEX_DATA_S3_KEY = "bronze/msci_index/ymd={{ ds }}/{{ ds }}_msci_index.json"
