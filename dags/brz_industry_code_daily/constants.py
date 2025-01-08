from datetime import datetime, timedelta

from airflow.models import Variable

# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
S3_BUCKET = Variable.get("S3_BUCKET")
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
# Get urls parameters
# I really wanted to avoid hardcoding it ...
# TODO: A pre-crawler DAG for the urls ?
MARKETS = {
    "kospi": ["MDC0201020101", "STK"],
    "kosdaq": ["MDC0201020506", "KSQ"],
}
