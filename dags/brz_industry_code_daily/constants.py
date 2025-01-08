from datetime import datetime, timedelta
from enum import Enum

from airflow.models import Variable


class AirflowParam(Enum):
    START_DATE = datetime(2025, 1, 6)


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")


class URLParam(Enum):
    MARKETS = {
        "kospi": ["MDC0201020101", "STK"],
        "kosdaq": ["MDC0201020506", "KSQ"],
    }
