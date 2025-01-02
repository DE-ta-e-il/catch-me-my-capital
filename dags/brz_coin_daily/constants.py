# 수집할 코인 심볼과 이름
SYMBOLS = {
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

# 코인 데이터 수집 임시 파일 경로
COIN_TMP_FILE_PATH = "/tmp/coin_data_{{ ds }}.csv"
# 코인 데이터 수집 데이터 파일 S3 키
COIN_DATA_S3_KEY = "bronze/coin_data/ymd={{ ds }}/{{ ds }}_coin_data.csv"
