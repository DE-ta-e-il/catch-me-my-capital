from airflow.models import Variable

# NOTE: Now uses AWS_CONN_VAR_S3_BUCKET from .env
S3_BUCKET = Variable.get("S3_BUCKET")
