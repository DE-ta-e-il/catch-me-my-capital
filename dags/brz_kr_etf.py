import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.trigger_rule import TriggerRule


def _is_holiday_today(year, month, today):
    url = (
        "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
    )
    params = {
        "solYear": year,
        "solMonth": month,
        "serviceKey": Variable.get("data_kr_service_key"),
    }

    response = requests.get(url, params=params)
    root = ET.fromstring(response.text)
    # 모든 <locdate> 태그의 값 추출
    locdates = [item.text for item in root.findall(".//locdate")]

    # today와 일치하는 값이 있는지 확인
    if today in locdates:
        return True
    else:
        return False


def choose_api_task(ds_nodash):
    date = datetime.strptime(ds_nodash, "%Y%m%d")
    year = date.year
    month = date.strftime("%m")
    if _is_holiday_today(year, month, ds_nodash):
        return "skip_task"

    cutoff_date = datetime(2020, 1, 1)
    if date < cutoff_date:
        return "fetch_etf_krx_web"
    else:
        return "fetch_etf_krx_api"


def fetch_old_etf_data_from_krx_web(ds_nodash):
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT04301",
        "locale": "ko_KR",
        "trdDd": ds_nodash,
        "share": 1,
        "money": 1,
        "csvxls_isNo": False,
    }
    headers = {
        "User-Agent": "PostmanRuntime/7.42.0",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020203",
    }

    response = requests.post(url, data=data, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch API: {response.status_code}, {response.text}")

    data = response.json()
    items = data.get("output")

    if not items:
        raise Exception(
            f"Data retrieval failed: 'output' is missing or empty. Full data: {data}"
        )

    return json.dumps(items)


def fetch_etf_data_from_krx_api(ds_nodash):
    url = "http://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService/getETFPriceInfo"
    params = {
        "serviceKey": Variable.get("data_kr_service_key"),
        "numOfRows": 1000,
        "pageNo": 1,
        "resultType": "json",
        "basDt": ds_nodash,
    }

    all_items = []

    while True:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch API: {response.status_code}, {response.text}"
            )

        data = response.json().get("response", {}).get("body", {})

        total_count = int(data.get("totalCount"))
        current_page = int(data.get("pageNo"))

        if total_count == 0:
            raise Exception(
                f"Data retrieval failed: 'totalCount' is missing or empty. Full data: {data}"
            )

        items = data.get("items", {}).get("item", [])
        all_items.extend(items)

        if len(all_items) >= total_count:
            break

        params["pageNo"] = current_page + 1

    return json.dumps(all_items)


def get_data_from_branch(task_instance):
    branch_result = task_instance.xcom_pull(task_ids="branch_api")

    if branch_result not in ["fetch_etf_krx_web", "fetch_etf_krx_api"]:
        raise ValueError(f"Unexpected branch result: {branch_result}")

    return task_instance.xcom_pull(task_ids=branch_result)


default_args = {
    "owner": "j-eum",
    "retries": 24,
    "retry_delay": timedelta(hours=1),
}

with DAG(
    dag_id="brz_kr_etf",
    default_args=default_args,
    description="한국거래소 ETF 종목별 시세",
    tags=["bronze", "ETF", "daily", "weekday"],
    schedule="0 0 * * 1-5",
    start_date=datetime(2015, 1, 1),
    catchup=False,
) as dag:
    branch_task = BranchPythonOperator(
        task_id="branch_api",
        python_callable=choose_api_task,
    )

    skip_task = DummyOperator(
        task_id="skip_task",
    )

    fetch_etf_krx_web = PythonOperator(
        task_id="fetch_etf_krx_web",
        python_callable=fetch_old_etf_data_from_krx_web,
    )

    fetch_etf_krx_api = PythonOperator(
        task_id="fetch_etf_krx_api",
        python_callable=fetch_etf_data_from_krx_api,
    )

    choose_data = PythonOperator(
        task_id="choose_data",
        python_callable=get_data_from_branch,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="bronze/kr_etf/date={{ ds }}/data.json",
        data="{{ ti.xcom_pull(task_ids='choose_data') }}",
        replace=True,
        aws_conn_id="aws_conn_id",
    )

    branch_task >> [skip_task, fetch_etf_krx_web, fetch_etf_krx_api]
    fetch_etf_krx_web >> choose_data
    fetch_etf_krx_api >> choose_data
    choose_data >> create_object
