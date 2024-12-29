from datetime import datetime, timedelta

from airflow.models import Variable

# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
S3_BUCKET = Variable.get("S3_BUCKET")
START_DATE = (datetime.now() - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
