from enum import StrEnum


class IntervalCode(StrEnum):
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    YEARLY = "A"


class StatCode(StrEnum):
    CENTRAL_BANK_POLICY_RATES = "902Y006"
    GDP_GROWTH_RATE = "902Y015"
