from enum import Enum, StrEnum


class Interval(Enum):
    DAILY = ("D", "daily")
    WEEKLY = ("W", "weekly")
    MONTHLY = ("M", "monthly")
    QUARTERLY = ("Q", "quarterly")
    YEARLY = ("A", "yearly")

    def __init__(self, code, label):
        self.code = code
        self.label = label


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
    S3_PARTITION_KEY = "ymd"


class ConnId(StrEnum):
    AWS = "aws_conn_id"
    BANK_OF_KOREA = "bank_of_korea_conn_id"


class Redshift:
    class SchemaName(StrEnum):
        SILVER = "silver"

    class TableName(StrEnum):
        DIM_CALENDAR = "dim_calendar"
        DIM_INDUSTRY_CODE = "dim_industry_code"
