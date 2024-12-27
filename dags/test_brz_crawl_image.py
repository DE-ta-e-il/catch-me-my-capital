import logging
import time
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from selenium import webdriver
from selenium.webdriver.common.by import By


def seltest():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")

    crawler = "crawler"  # container name for the crawler component
    url = "https://www.example.com"
    with webdriver.Remote(f"http://{crawler}:4444/wd/hub", options=options) as driver:
        driver.get(url)
        driver.implicitly_wait(1)
        time.sleep(1)
        title = driver.find_element(By.TAG_NAME, "h1")
        logging.info(title.text)


with DAG(
    dag_id="crawler_image_test",
    start_date=datetime(2024, 12, 12),
    catchup=False,
    schedule="0 1 * * *",
    tags=["test"],
) as dag:
    test_seq01 = PythonOperator(task_id="test_image_integ", python_callable=seltest)

    test_seq01
