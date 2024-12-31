import json

import requests
from airflow.hooks.base import BaseHook
from brz_economic_indicators_yearly.constants import IntervalCode


class BankOfKoreaHook(BaseHook):
    ENDPOINT = "StatisticSearch"

    def __init__(self, conn_id: str):
        super().__init__()
        self._conn_id = conn_id

    def _parse_conn(self):
        config = self.get_connection(self._conn_id)
        extra = json.loads(config.extra)

        base_url, api_key = extra.get("base_url"), extra.get("api_key")

        if not base_url or not api_key:
            raise ValueError("Invalid connection configuration")

        return base_url, api_key

    def get_conn(self):
        session = requests.Session()
        base_url, api_key = self._parse_conn()

        return session, base_url, api_key

    def _get_data(
        self,
        stat_code,
        date,
        interval,
        batch_size=100,
    ):
        session, base_url, api_key = self.get_conn()

        all_data = []
        start_index = 1

        while True:
            request_url = f"{base_url}/{self.ENDPOINT}/{api_key}/json/kr/{start_index}/{start_index+batch_size-1}/{stat_code}/{IntervalCode[interval]}/{date}/{date}"
            response = session.get(request_url)
            response.raise_for_status()

            data = response.json()

            # NOTE: 조회 기간에 해당하는 데이터가 없으면 "RESULT" 키를 포함하는 응답이 반환된다.
            if "RESULT" in data:
                raise ValueError("No data available for the query.")

            total_count = data[self.ENDPOINT].get("list_total_count", 0)

            if self.ENDPOINT in data and "row" in data[self.ENDPOINT]:
                batch_data = data[self.ENDPOINT]["row"]
                all_data.extend(batch_data)
                print(f"Fetched {len(batch_data)} records.")
            else:
                print("No more data or invalid response.")
                break

            start_index += batch_size
            if start_index > total_count:
                break

        print(all_data)
        return all_data

    def get_statistics(
        self,
        stat_code,
        date,
        interval,
        batch_size,
    ):
        return self._get_data(
            stat_code=stat_code,
            date=date,
            batch_size=batch_size,
            interval=interval,
        )
