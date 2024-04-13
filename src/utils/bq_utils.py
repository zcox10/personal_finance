import json
from datetime import timedelta, datetime as dt, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


class BqUtils:
    partition_formats = {
        "YYYYMMDD": "days",
        "YYYYMM": "months",
        "YYYYMMDDHH": "hours",
        "YYYYMMDDTHH": "hours",
    }

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

    def get_date(self, offset_days=0):
        """
        Get partition date according to current date (UTC). Use offset_days to add/subtract an offset.
        e.g. if today is 2023-03-20, and offset_days is set to 0 -> "2023-03-20"
        e.g. if today is 2023-03-20, and offset_days is set to -1 -> "2023-03-19"
        e.g. if today is 2023-03-20, and offset_days is set to 1 -> "2023-03-21"

        Args:
            offset_days (int): The offset to be applied to a given partition date

        Returns:
            datetime.date: The partition date formatted as "YYYY-MM-DD"
        """
        return (dt.now(timezone.utc) + timedelta(days=offset_days)).date()

    def get_partition_date(self, offset_days):
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

        date_utc = self.get_date(offset_days)

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

    def partition_format(self, table_id):
        """
        Type of the partition, if any. e.g. YYYYMMDD

        Args:
            table_id (str): Table id used to determine partition_format

        Returns:
            str: The partition format if exists, else None
        """
        for key in self.partition_formats:
            if table_id[-len(key) :] == key:
                return key
        return None

    def replace_table_suffix(self, table_id):
        """
        If table_id ends with any format in partition_formst, replace with just table_prefix; else table_prefix = table_id

        Args:
            table_id (str): Table id used to replace table_prefix

        Returns:
            str: Table prefix or table_id depending on if table is entered with a partition_format suffix or not.
        """

        partition_format = self.partition_format(table_id)
        if partition_format:
            return table_id[: -len(partition_format)]
        else:
            return table_id

    def get_table_partitions(self, project_id, dataset_id, table_id):
        # get table prefix by replacing partition_format e.g. YYYYMMDD, YYYYMM, YYYYMMDDHH
        table_prefix = self.replace_table_suffix(table_id)

        # query to get all partitions pertaining to a given format
        query = f"""
        SELECT table_id
        FROM `{project_id}.{dataset_id}.__TABLES__`
        WHERE 
          RTRIM(
            table_id,
            SPLIT(table_id, "_")[ORDINAL(ARRAY_LENGTH(SPLIT(table_id, "_")))]
            ) = "{table_prefix}"
        ORDER BY table_id
        """

        df = self.query(query)

        return [t for t in df["table_id"]]

    def get_latest_full_table_name(self, dataset_id, table_id):
        """
        Get the latest partition of a BigQuery full table {project_id}.{dataset_id}.{table_id} matching the specified prefix.

        Args:
            dataset_id (str): ID of the dataset containing the tables.
            table_id (str): Table id {table_id}_YYYYMMDD format used to filter tables.

        Returns:
            str: Full table ID of the latest partition.
        """

        table_prefix = self.replace_table_suffix(table_id)

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

        full_table_name = self.get_latest_full_table_name(dataset_id, table_id)
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

    def copy_bq_table(self, source_table, destination_table, write_disposition):
        """
        Copies data from a source BigQuery table to a destination BigQuery table.

        Args:
        - source_table (str): The fully qualified name (project.dataset.table) of the source table.
        - destination_table (str): The fully qualified name (project.dataset.table) of the destination table.
        - write_disposition (str): The write disposition for the copy job. Possible values are "WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY".

        Returns:
        - None: This function does not return anything. Prints a success message upon completion.
        """

        # Job configuration to copy the table
        job_config = bigquery.CopyJobConfig(write_disposition=write_disposition)

        # Start, then wait for job to complete
        job = self.bq_client.copy_table(source_table, destination_table, job_config=job_config)
        job.result()

        print(f"SUCCESS: `{source_table}` copied to `{destination_table}`\n")

    def delete_all_partitions(self, project_id, dataset_id, table_id, confirm):
        """
        Delete all partitions that follow a partition_format e.g. "YYYYMM", "YYYYMMDD", "YYYYMMDDHH", or "YYYYMMDDTHH"

        Args:
            project_id (str): ID of the BQ project containing the tables
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check, can include a partition_format suffix e.g. "{table_name}_YYYYMMDD"
            confirm (bool): if tables exists and confirm=True, confirm with Y/N if the table should be deleted
        """

        # get all table_id's that match the table prefix
        table_ids = self.get_table_partitions(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        if len(table_ids) == 0:  # if no tables exist, print FAILED statement
            print(f"FAILED: no tables match the {table_id} pattern")

        elif confirm:  # confirm whether to delete all tables or not, else delete all tables
            tables_to_delete = len(table_ids)
            self.user_prompt(
                prompt=f"Are you sure you want to delete {tables_to_delete} table(s): `{project_id}.{dataset_id}.{table_id}`?",
                action=lambda: self.delete_list_of_tables(project_id, dataset_id, table_ids, confirm=False),
                non_action_response=f"did not delete `{project_id}.{dataset_id}.{table_id}` table(s)",
            )
        else:  # delete the tables without confirmation
            self.delete_list_of_tables(project_id, dataset_id, table_ids, confirm=False)

    def delete_list_of_tables(self, project_id, dataset_id, table_ids, confirm):
        """
        Delete all a list of tables provided

        Args:
            project_id (str): ID of the BQ project containing the tables
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_ids (list): A list of table_id's to delete
            confirm (bool): if tables exists and confirm=True, confirm with Y/N if the table should be deleted. This will ask for confirmation to delete every table.
        """
        for _table in table_ids:
            self.delete_bq_table(project_id, dataset_id, _table, confirm)

    def user_prompt(self, prompt, action, non_action_response):
        """
        Prompts a user for a Y/N response. "Y" to continue and execute the action

        Args:
            prompt (str): A text message prompting the user for a given action e.g. "Do you want to continue?"
            action (str): ID of the BQ table to check.

        Returns:
            If user does not respond with "Y", returns nothing; else, executes the action
        """
        user_input = input(f"{prompt} (Y/N): ").strip().upper()
        print()
        if user_input == "Y":
            return action()
        else:
            print("REJECTED USER INPUT:", non_action_response)

    def delete_bq_table(self, project_id, dataset_id, table_id, confirm):
        """
        Deletes a BQ table

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check.
            confirm (bool): T/F to confirm if the user should be prompted to deleted the table.
        """

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)

        try:
            # Check if table exists. If does not exist, return
            if not self.does_bq_table_exist(project_id, dataset_id, table_id):
                print(f"`{full_table_name}` does not exist!")
                return

            # confirm with Y/N if user should delete the table
            elif confirm:
                self.user_prompt(
                    prompt=f"Are you sure you want to delete `{full_table_name}`?",
                    action=lambda: self.bq_client.delete_table(full_table_name),
                    non_action_response=f"did not delete `{full_table_name}`",
                )

            # if confirm != True, automatically delete the table without confirmation
            else:
                self.bq_client.delete_table(full_table_name)

            # ensure the table is deleted
            if not self.does_bq_table_exist(project_id, dataset_id, table_id):
                print(f"SUCCESS: `{full_table_name}` successfully deleted!")

        except NotFound:
            print(f"ERROR: The table, `{full_table_name}`, was not found.")
        except Exception as e:
            print("ERROR:", e)

    def create_empty_bq_table(self, project_id, dataset_id, table_id, table_description, table_schema, confirm):
        """
        Creates an empty BQ table based on the specified dataset_id, table_id, table_description, and table_schema

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check.
            table_description (str): Description used to describe the table.
            table_schema (str): Schema used to format the table.
            confirm (bool): if table exists and confirm=True, confirm with Y/N if the table should be deleted

        Returns:
            google.cloud.bigquery.table.Table: The created BigQuery table.
        """

        # determine if the table exists. If it does, delete it
        if self.does_bq_table_exist(project_id, dataset_id, table_id):
            self.delete_bq_table(project_id, dataset_id, table_id, confirm)

        # Define your BigQuery table
        table = bigquery.Table(f"{self.bq_client.project}.{dataset_id}.{table_id}", schema=table_schema)
        table.description = table_description

        create_table = self.bq_client.create_table(table)

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)
        print(f"SUCCESS: `{full_table_name}` successfully created!\n")

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

    def create_table(self, query, destination_table, write_disposition):
        """
        Execute a SQL query on a BigQuery table and write the result to a new BigQuery table.

        Args:
            bq_client (google.cloud.bigquery.client.Client): BigQuery client instance.
            query (str): SQL query to execute.
            destination_table (str): Destination table to store the result: "{project_id}.{dataset_id}.{table_id}"
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            google.cloud.bigquery.job.LoadJob: The job object representing the asynchronous query job.
        """
        job_config = bigquery.QueryJobConfig(destination=destination_table, write_disposition=write_disposition)

        query_job = self.bq_client.query(query, job_config=job_config)
        return query_job
