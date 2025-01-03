import logging

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def make_crawler(crawler_name, target_path):
    try:
        # Airflow connection을 사용하여 AWS 자격 증명 가져오기
        aws_hook = AwsBaseHook(aws_conn_id="aws_conn_id", client_type="glue")
        glue_client = aws_hook.get_client_type("glue", region_name="ap-northeast-2")

        # 크롤러 생성
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role="glue_role",
            Targets={"S3Targets": [{"Path": target_path}]},
            DatabaseName="team3-1-db",
            Description=f"{target_path}를 스캔하는 크롤러",
            Schedule="cron(0 0 * * ? *)",
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
        )

        logger.info(f"크롤러 생성 성공: {crawler_name}")
        return True

    except ClientError as e:
        logger.error(f"크롤러 생성 실패: {e}")
        raise ValueError(f"크롤러 생성 실패: {e}")
