from typing import List


def generate_table_names(interval: str, stat_name_list: List[str]):
    return [f"{interval}_{stat}".lower() for stat in stat_name_list]
