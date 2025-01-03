from enum import Enum


class Stat(Enum):
    CENTRAL_BANK_POLICY_RATES = "902Y006"
    GDP_GROWTH_RATE = "902Y015"

    def __init__(self, code):
        self.code = code
