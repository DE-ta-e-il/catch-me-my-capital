from datetime import datetime
from enum import Enum

from airflow.models import Variable


class AirflowParam(Enum):
    START_DATE = datetime(2025, 1, 1)


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")
