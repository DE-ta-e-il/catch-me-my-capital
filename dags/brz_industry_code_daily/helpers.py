import json

import boto3


def get_secret(secret_name, region_name="ap-northeast-2"):
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        else:
            return json.loads(
                response["SecretBinary"]
            )  # 바이너리 시크릿 사용 안하니까 그냥 raise?
    except Exception as e:
        raise Exception(f"Error fetching secret: {e}")
