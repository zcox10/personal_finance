import pandas as pd
from typing import List, Tuple
from utils.bq_utils import BqUtils


class DataQualityAlerts:

    def __init__(self, bq_client):
        self._bq = BqUtils(bq_client=bq_client)

    def get_single_bq_table_to_check(self, full_table_name: str) -> List[Tuple[str, str]]:
        split_table_identifiers = full_table_name.split(".")
        dataset_id = split_table_identifiers[1]
        table_id = split_table_identifiers[2]
        table_prefix = table_id[:-8]

        full_table_name = self._bq.get_latest_full_table_name(dataset_id, table_id)
        return (
            full_table_name,
            self._bq.get_table_suffix(full_table_name.split(".")[2], table_prefix),
        )

    def get_latest_bq_tables_to_check(self, full_table_name: str) -> List[Tuple[str, str]]:
        """
        0d signifies the latest partition date
        1d signifies the penultimate (second-to-latest) partition date

        Returns latest and penultimate table names and partitions.
        """
        split_table_identifiers = full_table_name.split(".")
        dataset_id = split_table_identifiers[1]
        table_id = split_table_identifiers[2]
        table_prefix = table_id[:-8]

        full_table_name_0d = self._bq.get_latest_full_table_name(dataset_id, table_id)
        table_suffix_0d = self._bq.get_table_suffix(full_table_name_0d.split(".")[2], table_prefix)

        full_table_name_1d = self._bq.get_second_latest_full_table_name(dataset_id, table_id)
        table_suffix_1d = self._bq.get_table_suffix(full_table_name_1d.split(".")[2], table_prefix)

        return [
            (full_table_name_0d, table_suffix_0d),
            (full_table_name_1d, table_suffix_1d),
        ]

    def financial_accounts_query(self) -> str:
        tables = self.get_latest_bq_tables_to_check("zsc-personal.personal_finance.financial_accounts_YYYYMMDD")

        # TODO: FIX path
        query = self._bq.sql_file_to_string("../src/queries/dqa_financial_accounts.sql")

        return query.format(
            full_table_name_0d=tables[0][0],
            table_suffix_0d=tables[0][1],
            full_table_name_1d=tables[1][0],
            table_suffix_1d=tables[1][1],
        )

    def generate_financial_accounts_df(self) -> pd.DataFrame:
        return self._bq.query(self.financial_accounts_query())
