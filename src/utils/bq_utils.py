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

    def replace_table_prefix(self, table_id):
        """
        If table_id ends with "_YYYYMMDD", replace with just table_prefix; else table_prefix = table_id

        Args:
            table_id (str): Table id used to replace table_prefix

        Returns:
            str: Table prefix or table_id depending on if table is entered with "_YYYYMMDD" suffix or not.
        """

        if table_id.endswith("_YYYYMMDD"):
            table_prefix = table_id[: -len("_YYYYMMDD")]
        else:
            table_prefix = table_id
        return table_prefix

    def get_latest_full_table_name(self, dataset_id, table_id):
        """
        Get the latest partition of a BigQuery full table {project_id}.{dataset_id}.{table_id} matching the specified prefix.

        Args:
            dataset_id (str): ID of the dataset containing the tables.
            table_id (str): Table id {table_id}_YYYYMMDD format used to filter tables.

        Returns:
            str: Full table ID of the latest partition.
        """

        table_prefix = self.replace_table_prefix(table_id)

        # Get all tables matching the prefix
        tables = self.bq_client.list_tables(self.bq_client.dataset(dataset_id))

        # Filter tables to only include those matching the prefix
        matching_tables = [table for table in tables if table.table_id.startswith(table_prefix)]

        # Get the latest partition
        latest_partition = max(matching_tables, key=lambda table: table.table_id.split("_")[-1]).full_table_id.replace(
            ":", "."
        )
        return latest_partition

    def get_latest_table_partition(self, dataset_id, table_id):
        """
        Get the latest partition for a table {table_id} in a BigQuery dataset.

        This function retrieves the full table name of the latest partitioned table
        matching the provided table prefix in the specified dataset. It then extracts
        and returns the partition identifier (table ID) from the full table name.

        Args:
            dataset_id (str): The ID of the BigQuery dataset.
            table_id (str): Table id {table_id}_YYYYMMDD format used to filter tables.

        Returns:
            str: The table ID (partition identifier) of the latest partitioned table
            matching the provided prefix in the specified dataset.
        """

        full_table_name = self.get_latest_full_table_name(self, dataset_id, table_id)
        table_id = full_table_name.split(".")[2]
        return table_id

    def concat_table_name(self, project_id, dataset_id, table_id):
        return project_id + "." + dataset_id + "." + table_id

    def does_bq_table_exist(self, project_id, dataset_id, table_id):
        """
        Returns True if a BQ table exists; else False

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check

        Returns:
            bool: True if a BQ table exists; else False
        """

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)

        # Check if the table exists
        try:
            self.bq_client.get_table(full_table_name)
            return True  # Table exists
        except Exception as e:
            if "Not found" in str(e):
                return False  # Table does not exist
            else:
                raise  # Other exception occurred, propagate it

    def delete_bq_table(self, project_id, dataset_id, table_id):
        """
        Deletes a BQ table

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check
        """

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)

        try:
            if not self.does_bq_table_exist(project_id, dataset_id, table_id):
                print(f"`{full_table_name}` does not exist!")
                return

            user_input = input(f"Are you sure you want to delete `{full_table_name}`? (Y/N): ").strip().upper()
            print()
            if user_input != "Y":
                return

            # Delete the table
            self.bq_client.delete_table(full_table_name)

            print(f"SUCCESS: `{full_table_name}` successfully deleted!")
        except NotFound:
            print(f"ERROR: The table, `{full_table_name}`, was not found.")
        except Exception as e:
            print("ERROR:", e)

    def create_empty_bq_table(self, project_id, dataset_id, table_id, table_description, table_schema):
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

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)

        if self.does_bq_table_exist(project_id, dataset_id, table_id):
            message = f"`{full_table_name}` already exists. Do you want to overwrite it? (Y/N): "
            user_input = input(message).strip().upper()
            if user_input != "Y":
                return
            else:
                self.delete_bq_table(project_id, dataset_id, table_id)

        # Define your BigQuery table
        table = bigquery.Table(f"{self.bq_client.project}.{dataset_id}.{table_id}", schema=table_schema)
        table.description = table_description

        create_table = self.bq_client.create_table(table)

        print(f"SUCCESS: `{full_table_name}` successfully created!")

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
