import logging

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def check_glue_crawler_exists(crawler_name):
    try:
        # Airflow connection을 사용하여 AWS 자격 증명 가져오기
        aws_hook = AwsBaseHook(aws_conn_id="aws_conn_id", client_type="glue")
        glue_client = aws_hook.get_client_type("glue", region_name="ap-northeast-2")

        # 크롤러 존재 여부 확인
        response = glue_client.get_crawler(Name=crawler_name)
        return True  # Crawler가 존재하면 True 반환

    except glue_client.exceptions.EntityNotFoundException:
        return False  # Crawler가 존재하지 않으면 False 반환

    except ClientError as e:
        logger.error(f"크롤러 확인 실패: {e}")
        raise ValueError(f"크롤러 확인 실패: {e}")


def decide_next_task(**kwargs):
    previous_result = kwargs["ti"].xcom_pull(task_ids="check_crawler_task")
    if previous_result:
        return "coin_glue_job"
    else:
        return "make_crawler_task"
