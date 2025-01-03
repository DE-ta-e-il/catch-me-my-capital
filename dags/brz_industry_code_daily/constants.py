from datetime import datetime, timedelta
from enum import Enum

from airflow.models import Variable


# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
# NOTE: Use CLASS.VAR_NAME.value to get these
class AirflowParam(Enum):
    START_DATE = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")


# Get urls parameters
# I really wanted to avoid hardcoding it ...
# TODO: A pre-crawler DAG for the urls ?
class MarketParam(Enum):
    MARKETS = {
        "kospi": ["MDC0201020101", "STK"],
        "kosdaq": ["MDC0201020506", "KSQ"],
    }
