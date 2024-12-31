from enum import StrEnum


class Interval(StrEnum):
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class Owner(StrEnum):
    DONGWON = "tunacome@gmail.com"
    DAMI = "mangodm.web3@gmail.com"
    JUNGMIN = "eumjungmin1@gmail.com"
    MINHYEOK = "tlsfk48@gmail.com"


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    LANDING = "landing"


class AwsConfig(StrEnum):
    S3_BUCKET_KEY = "s3_bucket"


class ConnId(StrEnum):
    AWS = "aws_conn_id"
    BANK_OF_KOREA = "bank_of_korea_conn_id"


S3_PARTITION_KEY = "ymd"
