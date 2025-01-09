from datetime import datetime
from enum import Enum

from airflow.models import Variable


class AirflowParam(Enum):
    START_DATE = datetime(2015, 1, 1)
    # (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


class ProvidersParam(Enum):
    S3_BUCKET = Variable.get("S3_BUCKET")


# Get urls parameters
# I really wanted to avoid hardcoding it ...
# TODO: A pre-crawler DAG for the urls ?
class URLParam(Enum):
    URLS_DICT = ["govt_bonds_kr", "govt_bonds_us", "corp_bonds_kr", "corp_bonds_us"]
