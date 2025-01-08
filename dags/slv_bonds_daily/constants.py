from datetime import datetime
from enum import Enum

from airflow.models import Variable


# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
# NOTE: Use CLASS.VAR_NAME.value to get these
class AirflowParam(Enum):
    # NOTE: For full-refresh test, glue job cost was taken into consideration😫
    START_DATE = datetime(2015, 1, 1)
    # Set it to False after first run
    FIRST_RUN = True


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")


class URLParam(Enum):
    URLS_DICT = ["govt_bonds_kr", "govt_bonds_us", "corp_bonds_kr", "corp_bonds_us"]
