import json
import time
import sys
import db_dtypes
import decimal
import pandas
from datetime import datetime as dt, timezone
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import LoadJobConfig


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

    def get_date(self, offset, partition_format):
        """
        Get partition date according to current date (UTC). Use offset to add/subtract an offset.
        e.g. if today is 2023-03-20, and offset is set to 0 -> "2023-03-20"
        e.g. if today is 2023-03-20, and offset is set to -1 -> "2023-03-19"
        e.g. if today is 2023-03-20, and offset is set to 1 -> "2023-03-21"

        Args:
            offset (int): The offset to be applied to a given partition date

        Returns:
            datetime.date: The partition date formatted as "YYYY-MM-DD"
        """

        if partition_format == "YYYYMMDD":
            return dt.now(timezone.utc) + relativedelta(days=offset)
        elif partition_format == "YYYYMM":
            return dt.now(timezone.utc) + relativedelta(months=offset)
        elif partition_format == "YYYYMMDDHH" or partition_format == "YYYYMMDDTHH":
            return dt.now(timezone.utc) + relativedelta(hours=offset)
        else:
            print(
                "\nMust input 'YYYYMMDD', 'YYYYMM', 'YYYYMMDDHH', or 'YYYYMMDDTHH' for partition_format"
            )
            return

    def get_partition_date(self, offset, partition_format):
        """
        Get partition date according to current date (UTC). Use offset to add/subtract an offset.
        e.g. if today is 2023-03-20, and offset is set to 0 -> "20230320"
        e.g. if today is 2023-03-20, and offset is set to -1 -> "20230319"
        e.g. if today is 2023-03-20, and offset is set to 1 -> "20230321"

        Args:
            offset (int): The offset to be applied to a given partition date

        Returns:
            str: The partition date formatted as "YYYYMMDD"
        """

        date_utc = self.get_date(offset, partition_format)

        # Format the UTC date and time as a string if needed
        if partition_format == "YYYYMMDD":
            return date_utc.strftime("%Y%m%d")
        elif partition_format == "YYYYMM":
            return date_utc.strftime("%Y%m")
        elif partition_format == "YYYYMMDDHH":
            return date_utc.strftime("%Y%m%d%H")
        elif partition_format == "YYYYMMDDTHH":
            return date_utc.strftime("%Y%m%dT%H")
        else:
            print(
                "\nMust input 'YYYYMMDD', 'YYYYMM', 'YYYYMMDDHH', or 'YYYYMMDDTHH' for partition_format"
            )
            return

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
        If table_id ends with any format in partition_format, replace with just table_prefix; else table_prefix = table_id

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

    def get_table_range_partitions(
        self, project_id, dataset_id, table_id, start_date=None, end_date=None
    ):
        """
        Retrieves full table IDs for a matching table pattern and partition range.

        Args:
            project_id (str): ID of the project containing the tables.
            dataset_id (str): ID of the dataset containing the tables.
            table_id (str): Table id in {table_id}_YYYYMMDD format used to filter tables.
            start_date (str, optional): Inclusive start date in 'YYYY-MM-DD' format. Defaults to None.
            end_date (str, optional): Exclusive end date in 'YYYY-MM-DD' format. Defaults to None.

        Returns:
            list: Full table IDs for a matching table pattern and partition range.
        """

        def format_date(date):
            return date.replace("-", "") if date else None

        def validate_dates(partition_format, start_date, end_date):
            if not partition_format:
                raise ValueError(
                    "Table does not have a valid partition format: 'YYYYMMDD', 'YYYYMM', 'YYYYMMDDHH'"
                )
            if start_date is None and end_date is None:
                raise ValueError("Need valid start and/or end partition")
            if start_date and len(partition_format) != len(start_date):
                raise ValueError("Table partition format does not match start date format")
            if end_date and len(partition_format) != len(end_date):
                raise ValueError("Table partition format does not match end date format")
            if start_date and end_date and start_date >= end_date:
                raise ValueError("Start date must be < end date")

        start_date = format_date(start_date)
        end_date = format_date(end_date)
        partition_format = self.partition_format(table_id)

        validate_dates(partition_format, start_date, end_date)

        table_prefix = self.replace_table_suffix(table_id)
        table_ids = self.get_table_partitions(project_id, dataset_id, table_id)

        def filter_table_ids(table_ids, start_date, end_date):
            final_table_ids = []
            for table_id in table_ids:
                partition = table_id[len(table_prefix) :]
                if len(partition) != len(partition_format):
                    raise ValueError(
                        f"Partition format, {partition_format}, does not match table partition's format, {table_id}"
                    )

                if (start_date is None or partition >= start_date) and (
                    end_date is None or partition < end_date
                ):
                    final_table_ids.append(table_prefix + partition)

            return final_table_ids

        return filter_table_ids(table_ids, start_date, end_date)

    def get_table_partitions(self, project_id, dataset_id, table_id):
        """
        Get the partitions that match a table pattern.
        For example, if "test_table_YYYYMMDD" is input, it retrieves all "test_table_" prefix tables

        Args:
            project_id (str): ID of the project containing the tables.
            dataset_id (str): ID of the dataset containing the tables.
            table_id (str): Table id {table_id}_YYYYMMDD format used to filter tables.

        Returns:
            list: Full table ID's for a matching table pattern.
        """

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
        latest_partition = max(
            matching_tables, key=lambda table: table.table_id.split("_")[-1]
        ).full_table_id.replace(":", ".")
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

    def update_single_table_partition(self, table_id, offset):
        """
        Update the partition of a single table based on the offset in days.

        Args:
            table_id (str): The ID of the table to update
            offset (int): The offset to be applied to a given partition date

        Returns:
            str: The updated table ID with the new partition.
        """

        partition_format = self.partition_format(table_id)
        partition_date = self.get_partition_date(offset, partition_format)
        table_prefix = self.replace_table_suffix(table_id)
        table_id = table_prefix + partition_date
        return table_id

    def update_many_table_partitions(self, table_list, offset):
        """
        Update the partitions of multiple tables in the list based on the offset in days.

        Args:
            table_list (List[str]): List of table IDs to update
            offset (int): The offset to be applied to a given partition date

        Returns:
            List[str]: List of updated table IDs with the new partitions
        """

        updated_tables = []
        for table in table_list:
            table_split = table.split(".")

            if self.partition_format(table_split[2]) is not None:
                table_split[2] = self.update_single_table_partition(table_split[2], offset)

            updated_tables.append(table_split)
        return updated_tables

    def update_table_schema_latest_partition(self, schema):
        """
        Updates a table's schema provided via BqTableSchemas with the "table_id" value representing the latest partition.
        For example, test_table_YYYYMMDD searches for the latest partition of test_table_YYYYMMDD (e.g. test_table_20240101)
        and replaces the "table_id" dict value with "test_table_20240101" from "test_table_YYYYMMDD".  Best when grabbing the latest
        partition

        Args:
            schema (dict): the schema represented in BqTableSchemas

        Returns:
            schema (dict): The updated table schema for the "table_id" value representing the latest partition
        """

        schema["table_id"] = self.get_latest_table_partition(
            schema["dataset_id"], schema["table_id"]
        )
        schema["full_table_name"] = (
            schema["project_id"] + "." + schema["dataset_id"] + "." + schema["table_id"]
        )
        return schema

    def update_table_schema_partition(self, schema, offset):
        """
        Updates a table's schema provided via BqTableSchemas with the "table_id" value representing a new partition.
        For example, test_table_YYYYMMDD is provided, then using offset = 0 and current date is "20240401",
        create a new value for schema["table_id"] = "test_table_20240401"

        Args:
            schema (dict): the schema represented in BqTableSchemas.
            offset (int): The offset to be applied to a given partition date

        Returns:
            schema (dict): The updated table schema for the "table_id" value representing the latest partition
        """
        # replace {table_name}_YYYYMMDD with specific partition e.g. {table_name}_20240401
        new_table_id = self.update_single_table_partition(schema["table_id"], offset)
        schema["table_id"] = new_table_id
        schema["full_table_name"] = (
            schema["project_id"] + "." + schema["dataset_id"] + "." + schema["table_id"]
        )
        return schema

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
                print("\n" + str(e))
                sys.exit(1)

    def check_dependencies(self, table_list, offset):
        """
        Check the dependencies of BigQuery tables.

        Args:
            table_list (List[str]): List of table IDs to check.
            offset (int): The offset to be applied to a given partition date

        Returns:
            None
        """

        print("Checking BQ table dependencies...")

        # update table names from {table_name}_YYYYMMDD to specific table name based on "offset" e.g. {table_name}_20240401
        updated_tables = self.update_many_table_partitions(table_list, offset)

        all_tables_exist = False
        count = 0
        while not all_tables_exist:
            table_statuses = []
            for table in updated_tables:
                table_status = self.does_bq_table_exist(table[0], table[1], table[2])
                table_statuses.append(table_status)
                if not table_status:
                    print(
                        f"`{self.concat_table_name(table[0], table[1], table[2])}` does not exist yet!"
                    )

            # mark True if all tables exist, else False
            all_tables_exist = all(table_statuses)

            # continue until all tables exist
            if not all_tables_exist:
                count += 1
                print("Not all tables exist yet. Sleeping 30 seconds.\n")
                time.sleep(30)
            else:
                print("BQ table dependencies passed!\n")

            if count >= 10:
                print("\nChecked table dependencies 10 times, exiting.")

    def load_df_to_bq(self, df, full_table_name, table_schema, write_disposition):
        """
        Loads a pandas DataFrame into a BigQuery table.

        Args:
            df (pandas.DataFrame): The DataFrame to be loaded into BigQuery.
            full_table_name (str): The full name (project_id.dataset_id.table_id) of the destination table in BigQuery.
            write_disposition (str): The write disposition for the job. Possible values are 'WRITE_EMPTY', 'WRITE_TRUNCATE', or 'WRITE_APPEND'.

        Returns:
            google.cloud.bigquery.job.LoadJob: The result of the load job.
        """

        job_config = LoadJobConfig()
        job_config.write_disposition = write_disposition

        if table_schema is not None:
            job_config.schema = table_schema
            df = self.cast_dataframe_for_parquet(df, table_schema)

        load_job = self.bq_client.load_table_from_dataframe(
            df, full_table_name, job_config=job_config
        )
        load_job.result()

        if load_job.state == "DONE":
            print(f"SUCCESS: df uploaded to `{full_table_name}`")

        return load_job

    def cast_dataframe_for_parquet(self, dataframe, schema):
        """Cast columns to needed dtype when writing parquet files.

        See: https://github.com/googleapis/python-bigquery-pandas/issues/421
        """

        columns = schema

        # Protect against an explicit None in the dictionary.
        columns = columns if columns is not None else []

        for column in columns:
            # Schema can be a superset of the columns in the dataframe, so ignore
            # columns that aren't present.
            column_name = column.get("name")
            if column_name not in dataframe.columns:
                continue

            # Skip array columns for now. Potentially casting the elements of the
            # array would be possible, but not worth the effort until there is
            # demand for it.
            if column.get("mode", "NULLABLE").upper() == "REPEATED":
                continue

            column_type = column.get("type", "").upper()
            if (
                column_type == "DATE"
                # Use extension dtype first so that it uses the correct equality operator.
                and db_dtypes.DateDtype() != dataframe[column_name].dtype
            ):
                cast_column = dataframe[column_name].astype(
                    dtype=db_dtypes.DateDtype(),
                    # Return the original column if there was an error converting
                    # to the dtype, such as is there is a date outside the
                    # supported range.
                    # https://github.com/googleapis/python-bigquery-pandas/issues/441
                    errors="ignore",
                )
            elif column_type in {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}:
                # decimal.Decimal does not support `None` or `pandas.NA` input, add
                # support here.
                # https://github.com/googleapis/python-bigquery-pandas/issues/719
                def convert(x):
                    if pandas.isna(x):  # true for `None` and `pandas.NA`
                        return decimal.Decimal("NaN")
                    else:
                        return decimal.Decimal(x)

                cast_column = dataframe[column_name].map(convert)
            else:
                cast_column = None

            if cast_column is not None:
                dataframe = dataframe.assign(**{column_name: cast_column})
        return dataframe

    def bq_table_has_data(self, project_id, dataset_id, table_id):
        """
        Returns True if a BQ table has at least 1 row of data; else False

        Args:
            project_id (str): ID of the BQ project containing the table.
            dataset_id (str): ID of the BQ dataset containing the table.
            table_id (str): ID of the BQ table to check

        Returns:
            bool: True if a BQ table has at least 1 row of data; else False
        """

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)

        if self.does_bq_table_exist(project_id, dataset_id, table_id):
            table = self.bq_client.get_table(full_table_name)
            if table.num_rows > 0:
                return True
            else:
                return False
        else:
            print(f"`{full_table_name}` does not exist!")
            return False

    def copy_bq_table(self, source_table, destination_table, write_disposition):
        """
        Copies data from a source BigQuery table to a destination BigQuery table.

        Args:
            source_table (str): The fully qualified name (project.dataset.table) of the source table.
            destination_table (str): The fully qualified name (project.dataset.table) of the destination table.
            write_disposition (str): The write disposition for the copy job. Possible values are "WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY".

        Returns:
            None: This function does not return anything. Prints a success message upon completion.
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

        Returns:
            None
        """

        # get all table_id's that match the table prefix
        table_ids = self.get_table_partitions(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        if len(table_ids) == 0:  # if no tables exist, delete single table
            self.delete_bq_table(project_id, dataset_id, table_id, confirm=confirm)

        elif confirm:  # confirm whether to delete all tables or not, else delete all tables
            tables_to_delete = len(table_ids)
            user_decision = self.user_prompt(
                prompt=f"Are you sure you want to delete {tables_to_delete} table(s): `{project_id}.{dataset_id}.{table_id}`?",
                action_response=f"deleting `{project_id}.{dataset_id}.{table_id}` table(s)",
                non_action_response=f"did not delete `{project_id}.{dataset_id}.{table_id}` table(s)",
            )
            if user_decision:
                self.delete_list_of_tables(project_id, dataset_id, table_ids, confirm=False)

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

        Returns:
            None
        """
        for _table in table_ids:
            self.delete_bq_table(project_id, dataset_id, _table, confirm)

    def user_prompt(self, prompt, action_response, non_action_response):
        """
        Prompts a user for a Y/N response. "Y" to continue and execute the action

        Args:
            prompt (str): A text message prompting the user for a given action e.g. "Do you want to continue?"
            action (str): ID of the BQ table to check.

        Returns:
            bool: True to continue with user action, else False
        """

        user_input = input(f"{prompt} (Y/N): ").strip().upper()
        if user_input == "Y":
            print("CONTINUE:", action_response, "\n")
            return True
        else:
            print("REJECTED USER INPUT:", non_action_response, "\n")
            return False

    def delete_bq_table(self, project_id, dataset_id, table_id, confirm):
        """
        Deletes a BQ table

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check.
            confirm (bool): T/F to confirm if the user should be prompted to deleted the table.

        Returns:
            None
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
                user_decision = self.user_prompt(
                    prompt=f"Are you sure you want to delete `{full_table_name}`?",
                    action_response=f"deleting `{full_table_name}`",
                    non_action_response=f"did not delete `{full_table_name}`",
                )

                if user_decision:
                    self.bq_client.delete_table(full_table_name)

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
            sys.exit(1)

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

    def create_empty_bq_table(
        self, project_id, dataset_id, table_id, table_description, table_schema, write_disposition
    ):
        """
        Creates an empty BQ table based on the specified dataset_id, table_id, table_description, and table_schema

        Args:
            dataset_id (str): ID of the BQ dataset containing the tables.
            table_id (str): ID of the BQ table to check.
            table_description (str): Description used to describe the table.
            table_schema (str): Schema used to format the table.
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            google.cloud.bigquery.table.Table: The created BigQuery table.
        """

        if self.does_bq_table_exist(project_id, dataset_id, table_id):
            if write_disposition == "WRITE_TRUNCATE":
                self.delete_bq_table(project_id, dataset_id, table_id, confirm=False)
            else:
                print(f"\n`{self.bq_client.project}.{dataset_id}.{table_id}` already exists")

        # Define your BigQuery table
        table = bigquery.Table(
            f"{self.bq_client.project}.{dataset_id}.{table_id}",
            schema=table_schema,
        )

        table.description = table_description

        create_table = self.bq_client.create_table(table)

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.concat_table_name(project_id, dataset_id, table_id)
        print(f"SUCCESS: `{full_table_name}` successfully created!\n")

        return create_table

    def create_query_bq_table(
        self,
        query,
        destination_table,
        write_disposition,
        renew_cache=True,
        table_description=None,
        table_schema=None,
    ):
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

        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition=write_disposition,
            use_query_cache=renew_cache,
        )

        query_job = self.bq_client.query(query, job_config=job_config)

        # Wait for the query to complete
        query_job.result()

        # Retrieve the created table
        table = self.bq_client.get_table(destination_table)

        # add description, if available
        if table_description is not None:
            table.description = table_description
            table = self.bq_client.update_table(table, ["description"])

        # add schema, if available
        if table_schema is not None:
            table.schema = table_schema
            table = self.bq_client.update_table(table, ["schema"])

        return query_job

    def sql_file_to_string(self, sql_path):
        """
        Converts a SQL file holding a SQL query to a string

        Args:
            sql_path (str): String containing a file path.

        Returns:
            String representation of a file's contents.
        """
        with open(sql_path, "r") as sql_file:
            return sql_file.read()
