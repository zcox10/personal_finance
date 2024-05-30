import time
import pandas as pd
from jobs.bq_table_schemas import BqTableSchemas
from utils.bq_utils import BqUtils


class PlaidTransactions:
    def __init__(self, bq_client, plaid_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__plaid_client = plaid_client
        self.__bq_tables = BqTableSchemas()

    def __create_items_dict(self, access_tokens):
        """
        Create a dict where each Plaid item is key and value is the associated access token

        Args:
            access_tokens (list): list of access tokens.
        Returns:
            dict: each Plaid item is key and value is the associated access token
        """

        access_tokens_dict = {}

        for token in access_tokens:
            item_dict = self.__plaid_client.get_item(token)
            access_tokens_dict[item_dict["item"]["item_id"]] = token

        return access_tokens_dict

    def get_latest_cursors(self, access_tokens):
        """
        Get the latest cursor for each access token and store in dataframe

        Args:

        Returns:
            pandas.DataFrame: Three fields -- (access_token, item_id, next_cursor)
        """

        access_tokens_dict = self.__create_items_dict(access_tokens)

        # define the table where cursors are stored
        plaid_cursors_bq = self.__bq.update_table_schema_latest_partition(
            schema=self.__bq_tables.plaid_cursors_YYYYMMDD()
        )

        if not self.__bq.does_bq_table_exist(
            plaid_cursors_bq["project_id"], plaid_cursors_bq["dataset_id"], plaid_cursors_bq["table_id"]
        ):
            print(f"`{plaid_cursors_bq['full_table_name']}` does not exist!")
            return None

        # query to grab each access_token / item_id utilizing "transactions" product and their associated latest cursor
        cursors_query = f"""
        SELECT DISTINCT 
          CAST(NULL AS STRING) AS access_token,
          item_id,
          next_cursor
        FROM `{plaid_cursors_bq['full_table_name']}`
        """

        cursors_df = self.__bq.query(cursors_query)

        for i, row in cursors_df.iterrows():
            row["access_token"] = access_tokens_dict[row["item_id"]]

        return cursors_df

    def create_cursors_bq_table(self, access_tokens, offset, write_disposition):
        """
        Creates an empty BigQuery table to store Plaid cursors. It retrieves Plaid access tokens
        and associated item IDs, then adds an empty cursor as the next cursor value to start fresh.

        Args:
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information, create new partition
        plaid_cursors_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.plaid_cursors_YYYYMMDD(), offset=offset
        )

        # # create empty table to store account data
        # self.__bq.create_empty_bq_table(
        #     project_id=plaid_cursors_bq["project_id"],
        #     dataset_id=plaid_cursors_bq["dataset_id"],
        #     table_id=plaid_cursors_bq["table_id"],
        #     table_description=plaid_cursors_bq["table_description"],
        #     table_schema=plaid_cursors_bq["table_schema"],
        #     write_disposition=write_disposition,
        # )

        # get plaid accounts. Stores access_token, item_id, and next cursor in df df
        accounts_df = self.__plaid_client.get_items_by_access_token(access_tokens, products=["transactions"])
        accounts_df = accounts_df[["item_id"]]

        # add empty cursor as next_cursor (fresh start)
        accounts_df["next_cursor"] = ""

        # table should already be empty, so use WRITE_TRUNCATE
        return self.__bq.load_df_to_bq(
            accounts_df, plaid_cursors_bq["full_table_name"], plaid_cursors_bq["table_schema"], "WRITE_TRUNCATE"
        )

    def add_cursor_to_bq(self, item_id, next_cursor, full_table_name, table_schema):
        """
        Updates a Plaid access token / item with the latest Plaid cursor

        Args:
            item_id (str): The item originating from Plaid
            next_cursor (str): The latest cursor from a Transactions Sync pull
            full_table_name (str): The full destination table {project_id}.{dataset_id}.{table_id} to upload the cursor entry to

        Returns:
            status (Any): The status of the BQ upload
        """

        # create df storing an item_id and next_cursor
        cursors_df = pd.DataFrame({"item_id": [item_id], "next_cursor": [next_cursor]})

        # Load the record to cursors temp BQ table. "WRITE_APPEND" because there are multiple individual uploads
        return self.__bq.load_df_to_bq(cursors_df, full_table_name, table_schema, "WRITE_APPEND")

    def create_temp_cursors_bq_table(self, write_disposition):
        """
        Creates an empty BigQuery table to store Plaid cursors temporarily. The latest cursor for each
        access token / item will be stored in this table until the job is complete

        Args:
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None
        """
        temp_cursors_bq = self.__bq_tables.temp_plaid_cursors()

        # create new temp_cursors BQ table
        self.__bq.create_empty_bq_table(
            project_id=temp_cursors_bq["project_id"],
            dataset_id=temp_cursors_bq["dataset_id"],
            table_id=temp_cursors_bq["table_id"],
            table_description=temp_cursors_bq["table_description"],
            table_schema=temp_cursors_bq["table_schema"],
            write_disposition=write_disposition,
        )

    def copy_temp_cursors_to_cursors_bq_table(self, offset, write_disposition):

        # get temp_cursors_bq table
        temp_cursors_bq = self.__bq_tables.temp_plaid_cursors()

        # get plaid_cursors_YYYYMMDD latest partition
        plaid_cursors_bq_latest = self.__bq.update_table_schema_latest_partition(
            schema=self.__bq_tables.plaid_cursors_YYYYMMDD()
        )

        query = f"""
        WITH
            temp_plaid_cursors AS (
            SELECT *
            FROM `{temp_cursors_bq["full_table_name"]}`
            )
            , latest_cursors AS (
            SELECT p.*
            FROM `{plaid_cursors_bq_latest["full_table_name"]}` p
            LEFT JOIN temp_plaid_cursors t
            USING (item_id)
            WHERE t.item_id IS NULL
            )
            SELECT *
            FROM temp_plaid_cursors
            UNION ALL 
            SELECT *
            FROM latest_cursors
        """

        # get BQ schema information
        plaid_cursors_bq_new = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.plaid_cursors_YYYYMMDD(), offset=offset
        )

        self.__bq.create_query_bq_table(
            query=query, destination_table=plaid_cursors_bq_new["full_table_name"], write_disposition=write_disposition
        )

    def __create_removed_df(self, item_id, removed_transactions, removed_accounts, partition_date):
        """
        Create a DataFrame containing removed transactions to filter out.

        Args:
            item_id (str): A singular item_id the removed transaction pertains to.
            removed_transactions (list): A list of transaction_id to remove.
            removed_accounts (list): A list of account_id pertaining to the transaction_id to remove.
            partition_date (str): The date the removed transactions were posted

        Returns:
            pandas.DataFrame: A DataFrame containing removed transaction_id's
        """

        # only create df if there are removed transactions
        if len(removed_transactions) == 0:
            return None

        # item_id and date_removed (partition_date) are singular values, create lists to match the number of removed transactions
        item_ids = [item_id] * len(removed_transactions)
        dates_removed = [partition_date] * len(removed_transactions)

        # add data to removed_df
        removed_df = pd.DataFrame(
            {
                "item_id": pd.Series(item_ids, dtype="str"),
                "account_id": pd.Series(removed_accounts, dtype="str"),
                "transaction_id": pd.Series(removed_transactions, dtype="str"),
                "date_removed": pd.Series(dates_removed, dtype="datetime64[ns]"),
            }
        )

        # remove any duplicates, if any
        removed_df.drop_duplicates(inplace=True)

        return removed_df

    def __create_transactions_df(self, transactions, item_id, status_type):
        """
        Create a DataFrame containing transaction data from create_transactions_df().

        Args:
            transactions (dict): A dictionary containing transaction information.
            item_id (str): The item originating from Plaid
            status_type (str): The status type to be assigned to all transactions.

        Returns:
            pandas.DataFrame: A DataFrame containing transaction data
        """
        # only create transactions df if data exists
        if len(transactions) == 0:
            return None

        account_ids = []
        account_owners = []
        amounts = []
        authorized_dates = []
        authorized_datetimes = []
        # categories = [] # deprecated for personal_finance_category
        # category_ids = [] # deprecated for personal_finance_category
        check_numbers = []
        counterparties = []
        dates = []
        datetimes = []
        currency_codes = []
        addresses = []
        cities = []
        regions = []
        postal_codes = []
        countries = []
        latitudes = []
        longitudes = []
        store_numbers = []
        merchant_entity_ids = []
        merchant_names = []
        names = []
        payment_channels = []
        reference_numbers = []
        ppd_ids = []
        payees = []
        by_order_ofs = []
        payers = []
        payment_methods = []
        payment_processors = []
        reasons = []
        pendings = []
        pending_transaction_ids = []
        personal_finance_category_confidence_levels = []
        personal_finance_category_detailed = []
        personal_finance_category_primaries = []
        transaction_codes = []
        transaction_ids = []
        # transaction_types = [] # deprecated for payment_channel
        unofficial_currency_codes = []
        websites = []
        statuses = []

        for t in transactions:
            account_ids.append(t["account_id"])
            account_owners.append(t["account_owner"])
            amounts.append(t["amount"])
            authorized_dates.append(t["authorized_date"])
            authorized_datetimes.append(t["authorized_datetime"])
            # categories.append(t["category"]) # deprecated
            # category_ids.append(t["category_id"]) # deprecated
            check_numbers.append(t["check_number"])
            counterparties.append(t["counterparties"])
            dates.append(t["date"])
            datetimes.append(t["datetime"])
            currency_codes.append(t["iso_currency_code"])

            # location
            addresses.append(t["location"]["address"])
            cities.append(t["location"]["city"])
            regions.append(t["location"]["region"])
            postal_codes.append(t["location"]["postal_code"])
            countries.append(t["location"]["country"])
            latitudes.append(t["location"]["lat"])
            longitudes.append(t["location"]["lon"])
            store_numbers.append(t["location"]["store_number"])

            # merchant
            merchant_entity_ids.append(t["merchant_entity_id"])
            merchant_names.append(t["merchant_name"])
            names.append(t["name"])
            payment_channels.append(t["payment_channel"])

            # payment meta
            reference_numbers.append(t["payment_meta"]["reference_number"])
            ppd_ids.append(t["payment_meta"]["ppd_id"])
            payees.append(t["payment_meta"]["payee"])
            by_order_ofs.append(t["payment_meta"]["by_order_of"])
            payers.append(t["payment_meta"]["payer"])
            payment_methods.append(t["payment_meta"]["payment_method"])
            payment_processors.append(t["payment_meta"]["payment_processor"])
            reasons.append(t["payment_meta"]["reason"])

            pendings.append(t["pending"])
            pending_transaction_ids.append(t["pending_transaction_id"])

            # personal finance
            personal_finance_category_confidence_levels.append(t["personal_finance_category"]["confidence_level"])
            personal_finance_category_detailed.append(t["personal_finance_category"]["detailed"])
            personal_finance_category_primaries.append(t["personal_finance_category"]["primary"])

            transaction_codes.append(t["transaction_code"])
            transaction_ids.append(t["transaction_id"])
            # transaction_types.append(t["transaction_type"])
            unofficial_currency_codes.append(t["unofficial_currency_code"])
            websites.append(t["website"])

            # transaction type
            statuses.append(status_type)

        # format counterparties array: create a list to store the formatted counterparties
        formatted_counterparties = []
        for counterparties_list in counterparties:
            formatted_counterparties_list = []
            for counterparty in counterparties_list:
                formatted_counterparties_list.append(
                    {
                        "entity_id": counterparty["entity_id"],
                        "name": counterparty["name"],
                        "type": counterparty["type"],
                        "confidence_level": counterparty["confidence_level"],
                        "logo_url": counterparty["logo_url"],
                        "website": counterparty["website"],
                    }
                )
            formatted_counterparties.append(formatted_counterparties_list)

        # only providing singular item_id, list size should be same as account_ids
        item_ids = [item_id] * len(account_ids)

        # add account data to accounts_df
        transactions_df = pd.DataFrame(
            {
                "item_id": pd.Series(item_ids, dtype="str"),
                "account_id": pd.Series(account_ids, dtype="str"),
                "transaction_id": pd.Series(transaction_ids, dtype="str"),
                "pending_transaction_id": pd.Series(pending_transaction_ids, dtype="str"),
                "is_pending": pd.Series(pendings, dtype="bool"),
                "account_owner": pd.Series(account_owners, dtype="str"),
                "status": pd.Series(statuses, dtype="str"),
                "transaction_date": pd.Series(dates, dtype="datetime64[ns]"),
                "transaction_datetime": pd.to_datetime(datetimes).tz_localize(None),
                "authorized_date": pd.Series(authorized_dates, dtype="datetime64[ns]"),
                "authorized_datetime": pd.to_datetime(authorized_datetimes).tz_localize(None),
                "amount": pd.Series(amounts, dtype="float64"),
                "currency_code": pd.Series(currency_codes, dtype="str"),
                "unofficial_currency_code": pd.Series(unofficial_currency_codes, dtype="str"),
                "personal_finance_category": [  # personal_finance_category struct with confidence_level, primary, and detailed fields
                    {
                        "primary": primary,
                        "detailed": detailed,
                        "confidence_level": confidence_level,
                    }
                    for primary, detailed, confidence_level in zip(
                        pd.Series(personal_finance_category_primaries, dtype="str"),
                        pd.Series(personal_finance_category_detailed, dtype="str"),
                        pd.Series(personal_finance_category_confidence_levels, dtype="str"),
                    )
                ],
                "payment_channel": pd.Series(payment_channels, dtype="str"),
                "merchant": [  # merchant struct with entity_id, merchant_name, and name fields
                    {
                        "entity_id": entity_id,
                        "merchant_name": merchant_name,
                        "name": name,
                        "website": website,
                    }
                    for entity_id, merchant_name, name, website in zip(
                        pd.Series(merchant_entity_ids, dtype="str"),
                        pd.Series(merchant_names, dtype="str"),
                        pd.Series(names, dtype="str"),
                        pd.Series(websites, dtype="str"),
                    )
                ],
                "counterparties": formatted_counterparties,  # counterparties array with name, type, logo_url, website, entity_id, and confidence_level fields
                "location": [  # location struct with address, city, region, postal_code, latitude, and longitude fields
                    {
                        "address": address,
                        "city": city,
                        "region": region,
                        "postal_code": postal_code,
                        "latitude": latitude,
                        "longitude": longitude,
                    }
                    for address, city, region, postal_code, latitude, longitude in zip(
                        pd.Series(addresses, dtype="str"),
                        pd.Series(cities, dtype="str"),
                        pd.Series(regions, dtype="str"),
                        pd.Series(postal_codes, dtype="str"),
                        pd.Series(latitudes, dtype="float64"),
                        pd.Series(longitudes, dtype="float64"),
                    )
                ],
                "check_number": check_numbers,
                "payment_meta": [  # payment_meta struct with reference_number, ppd_id, payee, by_order_of, payer, payment_method, payment_processor, and reason fields
                    {
                        "reference_number": reference_number,
                        "ppd_id": ppd_id,
                        "by_order_of": by_order_of,
                        "payee": payee,
                        "payer": payer,
                        "payment_method": payment_method,
                        "payment_processor": payment_processor,
                        "reason": reason,
                    }
                    for reference_number, ppd_id, by_order_of, payee, payer, payment_method, payment_processor, reason in zip(
                        pd.Series(reference_numbers, dtype="str"),
                        pd.Series(ppd_ids, dtype="str"),
                        pd.Series(by_order_ofs, dtype="str"),
                        pd.Series(payees, dtype="str"),
                        pd.Series(payers, dtype="str"),
                        pd.Series(payment_methods, dtype="str"),
                        pd.Series(payment_processors, dtype="str"),
                        pd.Series(reasons, dtype="str"),
                    )
                ],
                "transaction_code": transaction_codes,
                ### deprecated fields
                # "category": categories,
                # "category_id": category_ids,
                # "transaction_type": transaction_types,
            }
        )

        return transactions_df

    def create_empty_transactions_bq_table(self, offset, write_disposition):
        """
        Creates an empty plaid_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """
        # get BQ schema information
        plaid_transactions_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.plaid_transactions_YYYYMMDD(), offset=offset
        )

        # create empty table to store account data
        self.__bq.create_empty_bq_table(
            project_id=plaid_transactions_bq["project_id"],
            dataset_id=plaid_transactions_bq["dataset_id"],
            table_id=plaid_transactions_bq["table_id"],
            table_description=plaid_transactions_bq["table_description"],
            table_schema=plaid_transactions_bq["table_schema"],
            write_disposition=write_disposition,
        )

    def create_empty_removed_bq_table(self, offset, write_disposition):
        """
        Creates an empty plaid_removed_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """

        # get BQ schema information
        plaid_removed_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.plaid_removed_transactions_YYYYMMDD(), offset=offset
        )

        # create empty table to store account data
        self.__bq.create_empty_bq_table(
            project_id=plaid_removed_bq["project_id"],
            dataset_id=plaid_removed_bq["dataset_id"],
            table_id=plaid_removed_bq["table_id"],
            table_description=plaid_removed_bq["table_description"],
            table_schema=plaid_removed_bq["table_schema"],
            write_disposition=write_disposition,
        )

    def upload_transactions_df_to_bq(self, transactions_df, offset):
        """
        Upload the transactions_df to a pre-existing plaid_transactions_YYYYMMDD BQ table

        Args:
            transactions_df (pandas.DataFrame): the dataframe containing all plaid transactions
            offset (int): The offset to be applied to a given partition date

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_transactions_bq = self.__bq.update_table_schema_partition(
            self.__bq_tables.plaid_transactions_YYYYMMDD(),
            offset=offset,
        )

        # upload df to plaid_transactions_YYYYMMDD. "WRITE_APPEND" because multiple transaction_df's will be loaded
        return self.__bq.load_df_to_bq(
            transactions_df,
            plaid_transactions_bq["full_table_name"],
            plaid_transactions_bq["table_schema"],
            "WRITE_APPEND",
        )

    def upload_transactions_df_list_to_bq(self, transactions_df_list, offset, write_disposition):
        """
        Upload a list of transactions DataFrames to BigQuery.

        Args:
            transactions_df_list (List[pandas.DataFrame]): List of DataFrames containing transactions data.
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Write disposition for BigQuery ("WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY").

        Returns:
            None
        """

        # only upload transactions_df to BQ if there is at least one non-null df
        if len(transactions_df_list) > 0:
            concat_transactions_df = pd.concat(transactions_df_list)
            # self.create_empty_transactions_bq_table(offset, write_disposition)

            # print("SLEEP 5 SECONDS TO WAIT FOR plaid_transactions_YYYYMMDD creation\n")
            # time.sleep(5)

            self.upload_transactions_df_to_bq(concat_transactions_df, offset)

        else:
            print("No transactions present in concat_transactions_df")

    def upload_removed_df_to_bq(self, removed_df, offset):
        """
        Upload the removed_df to a pre-existing plaid_removed_transactions_YYYYMMDD BQ table

        Args:
            removed_df (pandas.DataFrame): the dataframe containing all plaid removed transactions
            offset (int): The offset to be applied to a given partition date

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_removed_bq = self.__bq.update_table_schema_partition(
            self.__bq_tables.plaid_removed_transactions_YYYYMMDD(),
            offset=offset,
        )

        # upload df to plaid_removed_transactions_YYYYMMDD. "WRITE_APPEND" because multiple transaction_df's will be loaded
        return self.__bq.load_df_to_bq(
            removed_df, plaid_removed_bq["full_table_name"], plaid_removed_bq["table_schema"], "WRITE_APPEND"
        )

    def upload_removed_df_list_to_bq(self, removed_df_list, offset, write_disposition):
        """
        Upload a list of removed transactions DataFrames to BigQuery.

        Args:
            removed_df_list (List[pandas.DataFrame]): List of DataFrames containing removed transactions data.
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Write disposition for BigQuery ("WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY").

        Returns:
            None
        """
        # only upload removed_df to BQ if there is at least one non-null df
        if len(removed_df_list) > 0:
            concat_removed_df = pd.concat(removed_df_list)
            # self.create_empty_removed_bq_table(offset, write_disposition)

            # print("SLEEP 5 SECONDS TO WAIT FOR plaid_removed_transactions_YYYYMMDD creation\n")
            # time.sleep(5)

            self.upload_removed_df_to_bq(concat_removed_df, offset)

        else:
            print("No removed transactions present in concat_removed_df")

    def generate_transactions_dfs(self, access_token, item_id, next_cursor, offset, add_test_transaction):
        """
        Generate transactions_df and removed_df from Plaid API data.

        Args:
            access_token (str): Plaid access token.
            item_id (str): Plaid item ID.
            next_cursor (str): Next cursor for pagination.
            offset (int): The offset to be applied to a given partition date.
            add_test_transaction (bool): Whether to add test transactions.

        Returns:
            Tuple[pandas.DataFrame, pandas.DataFrame]: A tuple containing transactions DataFrame and removed transactions DataFrame.
        """
        # retrieve transactions data from Plaid transactions_sync
        removed_transactions, removed_accounts, transactions_json, latest_cursor = (
            self.__plaid_client.get_transactions_data(access_token, next_cursor, add_test_transaction)
        )

        # append added_df and modified_df to transactions_df_list.
        transactions_df_list = []
        added_df = self.__create_transactions_df(transactions_json["added"], item_id, "ADDED")
        modified_df = self.__create_transactions_df(transactions_json["modified"], item_id, "MODIFIED")

        # Only add to transactions_df_list if data is present
        if added_df is not None:
            transactions_df_list.append(added_df)

        if modified_df is not None:
            transactions_df_list.append(modified_df)

        # create a final removed_df to store removed transactions. Returns None if no data available
        partition_date = self.__bq.get_date(offset=offset, partition_format="YYYYMMDD").date()
        removed_df = self.__create_removed_df(item_id, removed_transactions, removed_accounts, partition_date)

        # return if there is no data available
        if len(transactions_df_list) == 0:
            return None, removed_df

        # concat all transactions to main df
        transactions_df = pd.concat(transactions_df_list)

        # add next cursor back to next_cursor table
        temp_plaid_cursors_bq = self.__bq_tables.temp_plaid_cursors()

        # add cursor to temp_cursors table
        self.add_cursor_to_bq(
            item_id=item_id,
            next_cursor=latest_cursor,
            full_table_name=temp_plaid_cursors_bq["full_table_name"],
            table_schema=temp_plaid_cursors_bq["table_schema"],
        )

        print(f"SUCCESS: retrieved transactions for item_id: {item_id}")

        return transactions_df, removed_df

    def generate_transactions_df_list(self, latest_cursors_df, offset, add_test_transaction):
        """
        Generate a list of transactions DataFrames from Plaid API data.

        Args:
            latest_cursors_df (pandas.DataFrame): DataFrame containing the latest cursors for each item.
            offset (int): Number of days to offset in the table name.
            add_test_transaction (bool): Whether to add test transactions.

        Returns:
            Tuple[List[pandas.DataFrame], List[pandas.DataFrame]]: A tuple containing a list of transactions DataFrames and a list of removed transactions DataFrames.
        """

        transactions_df_list = []
        removed_df_list = []
        for i, row in latest_cursors_df.iterrows():

            transactions_df, removed_df = self.generate_transactions_dfs(
                access_token=row["access_token"],
                item_id=row["item_id"],
                next_cursor=row["next_cursor"],
                offset=offset,
                add_test_transaction=add_test_transaction,
            )

            if transactions_df is not None:
                transactions_df_list.append(transactions_df)

            if removed_df is not None:
                removed_df_list.append(removed_df)

        return transactions_df_list, removed_df_list
