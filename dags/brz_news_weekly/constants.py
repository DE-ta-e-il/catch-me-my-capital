from airflow.models import Variable

NYT_API_KEY = Variable.get("NYT_API_KEY")
NEWS_TMP_PATH = "/tmp/news_{{ logical_date.year }}_{{ logical_date.month }}.json"
NEWS_DATA_S3_KEY = "bronze/news/ymd={{ logical_date.year }}-{{ logical_date.strftime('%m') }}-01/{{ logical_date.year }}_{{ logical_date.strftime('%m') }}_news.json"
