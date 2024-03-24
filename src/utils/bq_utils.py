import json
from datetime import timedelta, datetime as dt, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


class BqUtils:
    def __init__(self, bq_client):
        self.bq_client = bq_client

    @staticmethod
    def pretty_print_response(response):
        """
        Print a JSON response in a human-readable format.

        Args:
            response (dict): JSON response to be printed.

        Returns:
            None
        """
        print(json.dumps(response, indent=2, sort_keys=True, default=str))

    @staticmethod
    def get_partition_date(offset_days=0):
        """
        Get partition date according to current date (UTC). Use offset_days to add/subtract an offset.
        e.g. if today is 2023-03-20, and offset_days is set to 0 -> "20230320"
        e.g. if today is 2023-03-20, and offset_days is set to -1 -> "20230319"
        e.g. if today is 2023-03-20, and offset_days is set to 1 -> "20230321"

        Args:
            offset_days (int): The offset to be applied to a given partition date

        Returns:
            str: The partition date formatted as "YYYYMMDD"
        """

        date_utc = (dt.now(timezone.utc) + timedelta(days=offset_days)).date()

        # Format the UTC date and time as a string if needed
        partition_date_string = date_utc.strftime("%Y%m%d")
        return partition_date_string

    def get_bq_client(self):
        """
        Get the BigQuery client instance.

        Returns:
            google.cloud.bigquery.client.Client: The BigQuery client instance.
        """
        return self.bq_client

    def get_latest_table_partition(self, dataset_id, table_prefix):
        """
        Get the latest partition of a BigQuery table matching the specified prefix.

        Args:
            dataset_id (str): ID of the dataset containing the tables.
            table_prefix (str): Prefix used to filter tables.

        Returns:
            str: Full table ID of the latest partition.
        """

        # Get all tables matching the prefix
        tables = self.bq_client.list_tables(self.bq_client.dataset(dataset_id))

        # Filter tables to only include those matching the prefix
        matching_tables = [
            table for table in tables if table.table_id.startswith(table_prefix)
        ]

        # Get the latest partition
        latest_partition = max(
            matching_tables, key=lambda table: table.table_id.split("_")[-1]
        ).full_table_id.replace(":", ".")
        return latest_partition

    def does_bq_table_exist(self, dataset_id, table_id):
        """
        Returns True if a BQ table exists; else False

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check

        Returns:
            bool: True if a BQ table exists; else False
        """

        # Construct the reference to the table
        table_ref = self.bq_client.dataset(dataset_id).table(table_id)

        # Check if the table exists
        try:
            self.bq_client.get_table(table_ref)
            return True  # Table exists
        except Exception as e:
            if "Not found" in str(e):
                return False  # Table does not exist
            else:
                raise  # Other exception occurred, propagate it

    def delete_bq_table(self, dataset_id, table_id):
        """
        Deletes a BQ table

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check
        """

        try:
            # Construct the reference to the table to be deleted
            table_ref = self.bq_client.dataset(dataset_id).table(table_id)

            if not self.does_bq_table_exist(dataset_id, table_id):
                print(f"`{table_ref}` does not exist!")
                return

            user_input = (
                input(f"Are you sure you want to delete `{table_ref}`? (Y/N): ")
                .strip()
                .upper()
            )
            print()
            if user_input != "Y":
                return

            # Delete the table
            self.bq_client.delete_table(table_ref)

            print(f"SUCCESS: `{table_ref}` successfully deleted!")
        except NotFound:
            print(f"ERROR: The table, `{table_ref}`, was not found.")
        except Exception as e:
            print("ERROR:", e)

    def create_empty_bq_table(
        self, dataset_id, table_id, table_description, table_schema
    ):
        """
        Creates an empty BQ table based on the specified dataset_id, table_id, table_description, and table_schema

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check.
            table_description (str): Description used to describe the table.
            table_schema (str): Schema used to format the table.

        Returns:
            google.cloud.bigquery.table.Table: The created BigQuery table.
        """

        table_ref = self.bq_client.dataset(dataset_id).table(table_id)

        if self.does_bq_table_exist(dataset_id, table_id):
            message = (
                f"`{table_ref}` already exists. Do you want to overwrite it? (Y/N): \n"
            )
            user_input = input(message).strip().upper()
            if user_input != "Y":
                return
            else:
                self.delete_bq_table(dataset_id, table_id)

        # Define your BigQuery table
        table = bigquery.Table(
            f"{self.bq_client.project}.{dataset_id}.{table_id}", schema=table_schema
        )
        table.description = table_description

        create_table = self.bq_client.create_table(table)

        print(f"SUCCESS: `{table_ref}` successfully created!")

        return create_table

    def query(self, query):
        """
        Execute a SQL query on a BigQuery table and return the result as a Pandas DataFrame.

        Args:
            bq_client (google.cloud.bigquery.client.Client): BigQuery client instance.
            query (str): SQL query to execute.

        Returns:
            pandas.DataFrame: Result of the query as a DataFrame.
        """
        query_job = self.bq_client.query(query)
        df = query_job.to_dataframe()
        return df

    def create_table(self, query, destination_table):
        """
        Execute a SQL query on a BigQuery table and write the result to a new BigQuery table.

        Args:
            bq_client (google.cloud.bigquery.client.Client): BigQuery client instance.
            query (str): SQL query to execute.
            destination_table (str): Destination table to store the result: "{project_id}.{dataset_id}.{table_id}"

        Returns:
            google.cloud.bigquery.job.LoadJob: The job object representing the asynchronous query job.
        """
        job_config = bigquery.QueryJobConfig(destination=destination_table)

        query_job = self.bq_client.query(query, job_config=job_config)
        return query_job

    def create_accounts_bq_table(self):
        """
        Create an empty BQ table to store Plaid financial account data. Stored as "zsc-personal.personal_finance.financial_accounts"

        Args:
            bq_client (google.cloud.bigquery.client.Client): BigQuery client instance.

        Returns:
            None
        """

        dataset_id = "personal_finance"
        table_id = "financial_accounts"
        table_description = "Stores all account data.  Field descriptions available at https://plaid.com/docs/api/accounts/#accounts-get-response-accounts-persistent-account-id"

        table_schema = [
            bigquery.SchemaField(
                name="item_id", field_type="STRING", mode="NULLABLE", description=""
            ),
            bigquery.SchemaField(
                name="persistent_account_id",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_id", field_type="STRING", mode="NULLABLE", description=""
            ),
            bigquery.SchemaField(
                name="account_mask",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_name",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_official_name",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_type",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_subtype",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="institution_id",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="institution_name",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="access_token",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="products", field_type="STRING", mode="REPEATED", description=""
            ),
            bigquery.SchemaField(
                name="billed_products",
                field_type="STRING",
                mode="REPEATED",
                description="",
            ),
        ]

        # Create accounts table
        self.create_empty_bq_table(
            dataset_id, table_id, table_description, table_schema
        )

    def create_cursors_bq_table(self, offset_days=0):
        """
        Create an empty BQ table to store Plaid account cursors in. Stored as "zsc-personal.personal_finance.account_cursors_YYYYMMDD"

        Args:
            bq_client (google.cloud.bigquery.client.Client): BigQuery client instance.

        Returns:
            None
        """

        partition = self.get_partition_date(offset_days=offset_days)
        dataset_id = "personal_finance"
        table_id = f"account_cursors_{partition}"
        table_description = "Stores item/access token cursors as of the latest run. Use latest partition to get most recently provided cursor"

        table_schema = [
            bigquery.SchemaField(
                name="item_id", field_type="STRING", mode="NULLABLE", description=""
            ),
            bigquery.SchemaField(
                name="access_token",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="cursor", field_type="STRING", mode="NULLABLE", description=""
            ),
        ]

        # Create cursors table
        self.create_empty_bq_table(
            dataset_id, table_id, table_description, table_schema
        )
