from enum import StrEnum


class Interval(StrEnum):
    DAILY = "daily"
    MONTHLY = "monthly"


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
