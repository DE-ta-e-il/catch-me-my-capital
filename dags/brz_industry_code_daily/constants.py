from datetime import datetime, timedelta
from enum import Enum

from airflow.models import Variable


class AirflowParam(Enum):
    START_DATE = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    FIRST_RUN = True


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")


class URLParam(Enum):
    MARKETS = {
        "kospi": ["MDC0201020101", "STK"],
        "kosdaq": ["MDC0201020506", "KSQ"],
    }
