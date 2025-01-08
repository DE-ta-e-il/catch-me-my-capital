from datetime import datetime
from enum import Enum

from airflow.models import Variable


# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
# NOTE: Use CLASS.VAR_NAME.value to get these
class AirflowParam(Enum):
    # NOTE: For full-refresh test, glue job cost was taken into consideration😫
    START_DATE = datetime(2025, 1, 1)
    # (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")
