from datetime import datetime, timedelta
from enum import Enum

from airflow.models import Variable


class AirflowParam(Enum):
    START_DATE = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")
