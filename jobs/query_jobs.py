from utils.bq_utils import BqUtils
from schemas.bq_table_schemas import BqTableSchemas


class QueryJobs:

    def __init__(self, bq_client):
        self._bq = BqUtils(bq_client=bq_client)
        self._bq_tables = BqTableSchemas()

    def create_tableau_table(self, sql_path, offset, write_disposition):
        tableau_bq = self._bq.update_table_schema_partition(
            schema=self._bq_tables.personal_finance_tableau_YYYYMMDD(), offset=offset
        )

        print(f"Creating table at `{tableau_bq.full_table_name}`")

        self._bq.create_query_bq_table(
            query=self._bq.sql_file_to_string(sql_path),
            destination_table=tableau_bq.full_table_name,
            write_disposition=write_disposition,
            renew_cache=True,
            table_description=tableau_bq.table_description,
            table_schema=tableau_bq.table_schema,
        )
