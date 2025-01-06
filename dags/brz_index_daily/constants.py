# 수집할 인덱스 심볼과 URL 코드
SYMBOLS = {
    "FTASEANAS": "961167",  # FTSE 아세안 종합지수(FTSE ASEAN All-Share Index)
    "SSEC": "40820",  # 상해 종합 지수 (중국, Shanghai Composite Index)
    "HSI": "179",  # 항생 지수 (홍콩, Hang Seng Index)
    "NSEI": "17940",  # 니프티 50 (인도, Nifty 50 Index)
    "IBOV": "17920",  # 이보베스파 (브라질, Ibovespa)
    "JTOPI": "41043",  # FTSE/JSE top 40 (남아공, FTSE/JSE Top 40 Index)
    "MIAP00000PUS": "942944",  # MSCI AC asia (아시아 중소국가 종합 지수, MSCI AC Asia Index)
    "SPX": "166",  # S&P 500 (미국, S&P 500 Index)
    "DJI": "169",  # 다우 존스 산업평균 (미국, Dow Jones Industrial Average)
    "FTSE": "27",  # 프린스트 종합 지수 (영국, FTSE All-Share Index)
    "DAX": "172",  # 도일 종합 지수 (독일, DAX Index)
    "CAC": "167",  # 카프 종합 지수 (프랑스, CAC 40 Index)
    "ASX": "171",  # 오스트레일리아 종합 지수 (오스트레일리아, ASX All Ordinaries Index)
}

# 인덱스 데이터 수집 임시 파일 경로
INDEX_TMP_FILE_PATH = "/tmp/index_data_{{ ds }}.json"
# 인덱스 데이터 수집 데이터 파일 S3 키
INDEX_DATA_S3_KEY = "bronze/index_data/ymd={{ ds }}/{{ ds }}_index_data.json"
