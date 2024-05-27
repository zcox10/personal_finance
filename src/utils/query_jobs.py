from utils.bq_utils import BqUtils
from jobs.bq_table_schemas import BqTableSchemas


class QueryJobs:

    def __init__(self, bq_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__bq_tables = BqTableSchemas()

    def create_tableau_table(self, write_disposition, offset):
        tableau_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.personal_finance_tableau_YYYYMMDD(), offset=offset
        )

        self.__bq.create_query_bq_table(
            query=self.__bq.sql_file_to_string("jobs/queries/tableau.sql"),
            destination_table=tableau_bq["full_table_name"],
            write_disposition=write_disposition,
            renew_cache=True,
            table_description=tableau_bq["table_description"],
            table_schema=tableau_bq["table_schema"],
        )
