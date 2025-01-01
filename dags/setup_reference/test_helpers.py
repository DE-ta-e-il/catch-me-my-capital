from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook


def example_creation(
    name, role, db, desc="This is a demonstration of glue crawler creation"
):
    targets = {
        "S3Targets": [
            {
                "Path": "string",
                "Exclusions": [
                    "string",
                ],
                "ConnectionName": "string",
                "SampleSize": 123,
                "EventQueueArn": "string",
                "DlqEventQueueArn": "string",
            },
        ],
        "CatalogTargets": [
            {
                "DatabaseName": "string",
                "Tables": [
                    "string",
                ],
                "ConnectionName": "string",
                "EventQueueArn": "string",
                "DlqEventQueueArn": "string",
            },
        ],
    }
    hook = GlueCrawlerHook(aws_conn_id="aws_conn_id")
    hook.create_crawler(
        Name=name,
        Role=role,
        DatabaseName=db,
        Description=desc,
        Targets=targets,
        Schedule="string",
        Classifiers=[
            "string",
        ],
        TablePrefix="string",
        SchemaChangePolicy={
            "UpdateBehavior": "LOG" | "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG" | "DELETE_FROM_DATABASE" | "DEPRECATE_IN_DATABASE",
        },
        RecrawlPolicy={
            "RecrawlBehavior": "CRAWL_EVERYTHING"
            | "CRAWL_NEW_FOLDERS_ONLY"
            | "CRAWL_EVENT_MODE"
        },
        LineageConfiguration={"CrawlerLineageSettings": "ENABLE" | "DISABLE"},
        LakeFormationConfiguration={
            "UseLakeFormationCredentials": True | False,
            "AccountId": "string",
        },
        Configuration="string",
        CrawlerSecurityConfiguration="string",
        Tags={"string": "string"},
    )


# Function to start Glue Crawler
def run_glue_crawler(crawler_name):
    """
    wait_for_crawler_completion(name, poll_interval=5): returns crawler status(str)
    has_crawler(name): returns boolean for whether it exists\n
    update_crawler(**kwargs) -> edit config\n
    get_crawler(name) -> config listing\n
    create_crawler(**kwargs): returns name of the crawler -> \n
    Name(str), Role(str), DatabaseName(str), Description=(str), Targets=(Object)\n
    For 'Targets', see: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/create_crawler.html
    """
    hook = GlueCrawlerHook(aws_conn_id="aws_conn_id")
    hook.start_crawler(crawler_name)
    hook.wait_for_crawler_completion(crawler_name, poll_interval=3)
