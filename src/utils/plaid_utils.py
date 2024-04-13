import json
import pandas as pd
from google.api_core.exceptions import NotFound
from utils.bq_utils import BqUtils
from sql.bq_table_schemas import BqTableSchemas

from plaid.configuration import Configuration
from plaid.api_client import ApiClient

# from plaid.exceptions import OpenApiException
# from plaid.exceptions import ApiAttributeError
# from plaid.exceptions import ApiTypeError
# from plaid.exceptions import ApiValueError
# from plaid.exceptions import ApiKeyError
from plaid.exceptions import ApiException

from plaid.api import plaid_api

from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.country_code import CountryCode
from plaid.model.products import Products
from plaid.model.investments_transactions_get_request_options import (
    InvestmentsTransactionsGetRequestOptions,
)
from plaid.model.investments_transactions_get_request import (
    InvestmentsTransactionsGetRequest,
)
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest


class PlaidUtils:
    def __init__(self, bq_client, client_id, client_secret, host):
        self.bq_client = bq_client
        self.bq = BqUtils(bq_client=bq_client)
        self.plaid_client = self.authenticate(client_id, client_secret, host)
        self.bq_tables = BqTableSchemas()

    def authenticate(self, client_id, client_secret, host):
        """
        Authenticate and create a Plaid API client.

        Args:
            client_id (str): Client ID for Plaid API authentication.
            client_secret (str): Client secret for Plaid API authentication.
            host (str): Host URL for Plaid API.

        Returns:
            plaid_api.PlaidApi: Plaid API client.
        """

        configuration = Configuration(host=host, api_key={"clientId": client_id, "secret": client_secret})

        api_client = ApiClient(configuration)
        client = plaid_api.PlaidApi(api_client)

        return client

    def get_institution_by_id(self, institution_id, plaid_country_codes):
        """
        Get institution details by ID using the Plaid API.

        Args:
            plaid_client (plaid_api.PlaidApi): Plaid API client.
            institution_id (str): ID of the institution to retrieve details for.
            plaid_country_codes (list of str): List of country codes.

        Returns:
            dict: Details of the institution.
        """

        try:
            request = InstitutionsGetByIdRequest(
                institution_id=institution_id,
                country_codes=list(map(lambda x: CountryCode(x), plaid_country_codes)),
            )

            response = self.plaid_client.institutions_get_by_id(request)

            return response.to_dict()

        except ApiException as e:
            return json.loads(e.body)

    def get_accounts(self, access_token):
        """
        Get accounts associated with a Plaid access token.

        Args:
            plaid_client (plaid_api.PlaidApi): Plaid API client.
            access_token (str): Access token for Plaid API authentication.

        Returns:
            dict: Details of the accounts.
        """

        try:
            request = AccountsGetRequest(access_token=access_token)
            response = self.plaid_client.accounts_get(request)
            return response.to_dict()

        except ApiException as e:
            return json.loads(e.body)

    def add_accounts_to_bq(self, access_token, plaid_country_codes):
        """
        Add accounts data to a defined BQ table.

        Args:
            bq_client (google.cloud.bigquery.client.Client): BigQuery client instance.
            access_token (str): Plaid access token.

        Returns:
            None
        """

        responses = self.get_accounts(access_token)

        item_ids = []
        persistent_account_ids = []
        account_ids = []
        account_masks = []
        account_names = []
        account_official_names = []
        account_types = []
        account_subtypes = []
        institution_ids = []
        institution_names = []
        access_tokens = []
        products = []
        billed_products = []

        # print(responses)

        for r in responses["accounts"]:
            try:
                persistent_account_ids.append(r["persistent_account_id"])
            except:
                persistent_account_ids.append(None)

            account_ids.append(r["account_id"])
            account_masks.append(r["mask"])
            account_names.append(r["name"])
            account_official_names.append(r["official_name"])
            account_types.append(r["type"])
            account_subtypes.append(r["subtype"])

            item_ids.append(responses["item"]["item_id"])
            institution_ids.append(responses["item"]["institution_id"])
            billed_products.append(responses["item"]["billed_products"])
            products.append(responses["item"]["products"])
            access_tokens.append(access_token)

        # get institution names
        distinct_institutions = list(set(institution_ids))

        institutions_dict = {}
        for institution_id in distinct_institutions:
            institution = self.get_institution_by_id(institution_id, plaid_country_codes)
            institution_name = institution["institution"]["name"]
            institutions_dict[institution_id] = institution_name

        for id in institution_ids:
            try:
                institution_names.append(institutions_dict[id])
            except:
                institution_names.append(None)

        # add account data to accounts_df
        accounts_df = pd.DataFrame(
            {
                "item_id": item_ids,
                "persistent_account_id": persistent_account_ids,
                "account_id": account_ids,
                "account_mask": account_masks,
                "account_name": account_names,
                "account_official_name": account_official_names,
                "account_type": account_types,
                "account_subtype": account_subtypes,
                "institution_id": institution_ids,
                "institution_name": institution_names,
                "access_token": access_tokens,
                "products": products,
                "billed_products": billed_products,
            }
        )

        # Define your BigQuery table reference
        plaid_accounts_bq = self.bq_tables.plaid_accounts()

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.bq.concat_table_name(
            plaid_accounts_bq["project_id"], plaid_accounts_bq["dataset_id"], plaid_accounts_bq["table_id"]
        )

        # get
        try:
            accts_q = f"""
            SELECT DISTINCT
              persistent_account_id,
              account_id
            FROM `{full_table_name}`
            """
            get_accts_df = self.bq.query(accts_q)

            get_persistent_account_ids = [i for i in get_accts_df["persistent_account_id"].unique() if i is not None]
            duplicate_persistent_account_ids = [i for i in persistent_account_ids if i in get_persistent_account_ids]
            if len(duplicate_persistent_account_ids) > 0:
                print("persistsent_account_id(s) already present in the data:")
                for i in duplicate_persistent_account_ids:
                    print(f"- {i}")

                self.bq.user_prompt(
                    prompt="\nDo you want to continue?",
                    action=lambda: print("CONTINUE: adding persistent_account_id(s) that are already present"),
                    non_action_response="did not add persistent_account_id(s)",
                )

            get_account_ids = [i for i in get_accts_df["account_id"].unique() if i is not None]
            duplicate_account_ids = [i for i in account_ids if i in get_account_ids]
            if len(duplicate_account_ids) > 0:
                print("account_id(s) already present in the data:")
                for i in duplicate_account_ids:
                    print(f"- {i}")
                self.bq.user_prompt(
                    prompt="\nDo you want to continue?",
                    action=lambda: print("CONTINUE: adding account_id(s) that are already present"),
                    non_action_response="did not add account_id(s)",
                )
        except NotFound:
            print(f"The table, `{full_table_name}`, was not found.")
            return
        except Exception as e:
            print(e)
            return

        # Load the DataFrame into the BigQuery table (commit data to storage immediately)
        job = self.bq_client.load_table_from_dataframe(accounts_df, full_table_name)

        # Wait for job to complete
        status = job.result()
        print(f"Accounts successfully added to `{full_table_name}` -> access_token: {access_token}")

        return

    def get_latest_cursors(self):
        """
        Get the latest cursor for each access token and store in dataframe

        Args:

        Returns:
            pandas.DataFrame: Three fields -- (access_token, item_id, next_cursor)
        """

        # define the table where cursors are stored
        plaid_cursors_bq = self.bq_tables.plaid_cursors_YYYYMMDD()

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.bq.concat_table_name(
            plaid_cursors_bq["project_id"],
            plaid_cursors_bq["dataset_id"],
            self.bq.get_latest_table_partition(plaid_cursors_bq["dataset_id"], plaid_cursors_bq["table_id"]),
        )

        # query to grab each access_token / item_id and their associated latest cursor
        cursors_query = f"""
        SELECT DISTINCT 
          access_token,
          item_id,
          next_cursor
        FROM `{full_table_name}`
        """

        cursors_df = self.bq.query(cursors_query)
        return cursors_df

    def get_transactions(self, access_token, item_id, next_cursor, offset_days):
        """
        Retrieves transactions and handles pagination.

        Retrieves transactions using the provided access token and handles pagination if necessary.

        Args:
            access_token (str): Access token for Plaid API.
            item_id (str): ID of the item associated with the transactions.
            next_cursor (str): Cursor for fetching the next set of transactions.
            offset_days (int): The number of days to offset for the date_removed field

        Returns:
            tuple: A tuple containing:
                - pandas.DataFrame: A DataFrame containing transaction data.
                - set: A set containing transaction IDs of removed transactions.

        Raises:
            ApiException: If an error occurs during the API request.
        """

        # get transactions
        # try:
        has_more = True
        removed_transactions = []
        removed_accounts = []
        transactions_df_list = []

        while has_more:
            # Fetch transactions for the account
            request = TransactionsSyncRequest(access_token=access_token, cursor=next_cursor, count=500)

            response = self.plaid_client.transactions_sync(request)
            has_more = response["has_more"]
            next_cursor = response["next_cursor"]

            transactions_json = response.to_dict()

            # self.bq.pretty_print_response(transactions_json)

            if len(transactions_json["added"]) > 0:
                added_df = self.create_transactions_df(transactions_json["added"], item_id, "ADDED")
                transactions_df_list.append(added_df)

            if len(transactions_json["modified"]) > 0:
                modified_df = self.create_transactions_df(transactions_json["modified"], item_id, "MODIFIED")
                transactions_df_list.append(modified_df)

            # add all removed transactions to removed_transactions list
            if len(transactions_json["removed"]) > 0:
                for r in transactions_json["removed"]:
                    removed_accounts.append(r["account_id"])
                    removed_transactions.append(r["transaction_id"])

            # add test transaction id
            removed_transactions.append("test_transaction_id")
            removed_accounts.append("test_account_id")

        # create a final removed_df to store removed transactions
        partition_date = self.bq.get_date(offset_days=offset_days)
        removed_df = self.create_removed_df(item_id, removed_transactions, removed_accounts, partition_date)

        # concat all transactions to main df
        transactions_df = pd.concat(transactions_df_list)

        # add next cursor back to next_cursor table
        temp_plaid_cursors_bq = self.bq_tables.temp_plaid_cursors()

        # concat "{project_id}.{dataset_id}.{table_id}"
        temp_cursors_table_name = self.bq.concat_table_name(
            temp_plaid_cursors_bq["project_id"],
            temp_plaid_cursors_bq["dataset_id"],
            temp_plaid_cursors_bq["table_id"],
        )

        # add cursor to temp_cursors table
        self.add_cursor_to_bq(
            item_id=item_id,
            access_token=access_token,
            next_cursor=next_cursor,
            full_table_name=temp_cursors_table_name,
        )

        print(f"SUCCESS: retrieved transacstions for item_id: {item_id}")

        return transactions_df, removed_df

        # except ApiException as e:
        #     print("ERROR:", e)

    def get_access_tokens(self):
        """
        Gather all Plaid access tokens and items into a df. Used in create_cursors_bq_table()

        Args:

        Returns:
            pandas.DataFrame: Details of the items.
        """

        # get BQ schema for plaid_accounts table
        plaid_accounts_bq = self.bq_tables.plaid_accounts()

        # concat "{project_id}.{dataset_id}.{table_id}"
        full_table_name = self.bq.concat_table_name(
            plaid_accounts_bq["project_id"],
            plaid_accounts_bq["dataset_id"],
            plaid_accounts_bq["table_id"],
        )

        # generate query to pull access_token and item_id, then return as a df
        query = f"""
        SELECT DISTINCT
          access_token,
          item_id
        FROM `{full_table_name}`
        """

        return self.bq.query(query)

    def create_accounts_bq_table(self, access_tokens, plaid_country_codes, confirm):
        """
        Creates an empty BigQuery table to store Plaid account data. Uses the provided access tokens
        to fetch account information from the Plaid API and adds it to the BigQuery table.

        Args:
            access_tokens (list): A list of access tokens for accessing Plaid API and fetching account data.
            plaid_country_codes (list): A list of Plaid country codes to specify the country for which accounts are fetched.
            confirm (bool): if table exists and confirm=True, confirm with Y/N if the pre-existing table should be deleted

        Returns:
            None
        """

        # get BQ schema information
        plaid_accounts_bq = self.bq_tables.plaid_accounts()

        # create empty table to store account data
        self.bq.create_empty_bq_table(
            project_id=plaid_accounts_bq["project_id"],
            dataset_id=plaid_accounts_bq["dataset_id"],
            table_id=plaid_accounts_bq["table_id"],
            table_description=plaid_accounts_bq["table_description"],
            table_schema=plaid_accounts_bq["table_schema"],
            confirm=confirm,
        )

        # add access tokens to new empty accounts table
        for token in access_tokens:
            self.add_accounts_to_bq(
                access_token=token,
                plaid_country_codes=plaid_country_codes,
            )

        # Final print statement
        full_table_name = self.bq.concat_table_name(
            plaid_accounts_bq["project_id"], plaid_accounts_bq["dataset_id"], plaid_accounts_bq["table_id"]
        )
        print(f"SUCCESS: all access tokens added to `{full_table_name}`\n")

    def create_cursors_bq_table(self, offset_days, confirm):
        """
        Creates an empty BigQuery table to store Plaid cursors. It retrieves Plaid access tokens
        and associated item IDs, then adds an empty cursor as the next cursor value to start fresh.

        Args:
            offset_days (int): The number of days to offset when determining the partition date for the plaid_cursors_YYYYMMDD table.
            confirm (bool): if table exists and confirm=True, confirm with Y/N if the pre-existing table should be deleted.

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_cursors_bq = self.bq_tables.plaid_cursors_YYYYMMDD()
        partition_date = self.bq.get_partition_date(offset_days=offset_days)
        table_prefix = self.bq.replace_table_suffix(plaid_cursors_bq["table_id"])
        table_id = table_prefix + partition_date

        # create empty table to store account data
        self.bq.create_empty_bq_table(
            project_id=plaid_cursors_bq["project_id"],
            dataset_id=plaid_cursors_bq["dataset_id"],
            table_id=table_id,
            table_description=plaid_cursors_bq["table_description"],
            table_schema=plaid_cursors_bq["table_schema"],
            confirm=confirm,
        )

        # get plaid accounts. Stores access_token, item_id, and next cursor in df df
        accounts_df = self.get_access_tokens()

        # add empty cursor as next_cursor (fresh start)
        accounts_df["next_cursor"] = ""

        full_table_name = self.bq.get_latest_full_table_name(
            plaid_cursors_bq["dataset_id"], plaid_cursors_bq["table_id"]
        )

        return self.bq_client.load_table_from_dataframe(accounts_df, full_table_name)

    def add_cursor_to_bq(self, item_id, access_token, next_cursor, full_table_name):
        """
        Updates a Plaid access token / item with the latest Plaid cursor

        Args:
            item_id (str): The item originating from Plaid
            access_token (str): The access_token associated with a Plaid item
            next_cursor (str): The latest cursor from a Transactions Sync pull
            full_table_name (str): The full destination table {project_id}.{dataset_id}.{table_id} to upload the cursor entry to

        Returns:
            status (Any): The status of the BQ upload
        """

        # create df storing an item_id, access_token, and next_cursor
        cursors_df = pd.DataFrame({"item_id": [item_id], "access_token": [access_token], "next_cursor": [next_cursor]})

        # Load the DataFrame into the BigQuery table (commit data to storage immediately)
        job = self.bq_client.load_table_from_dataframe(cursors_df, full_table_name)

        # Wait for job to complete
        status = job.result()
        print(f"Cursors successfully added to `{full_table_name}`")
        return status

    def create_temp_cursors_bq_table(self, confirm):
        """
        Creates an empty BigQuery table to store Plaid cursors temporarily. The latest cursor for each
        access token / item will be stored in this table until the job is complete

        Args:
            confirm (bool): if table exists and confirm=True, confirm with Y/N if the pre-existing table should be deleted

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """
        temp_cursors_bq = self.bq_tables.temp_plaid_cursors()

        # create new temp_cursors BQ table
        self.bq.create_empty_bq_table(
            project_id=temp_cursors_bq["project_id"],
            dataset_id=temp_cursors_bq["dataset_id"],
            table_id=temp_cursors_bq["table_id"],
            table_description=temp_cursors_bq["table_description"],
            table_schema=temp_cursors_bq["table_schema"],
            confirm=confirm,
        )

    def copy_temp_cursors_to_cursors_bq_table(self, offset_days, write_disposition):
        """
        Copies data from a temporary Plaid cursors table to a permanent Plaid cursors table in BigQuery.

        Args:
            offset_days (int): The number of days to offset when determining the partition date for the destination table.
            write_disposition (str): The write disposition for the copy job. Possible values are "WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY".

        Returns:
            None: This function does not return anything. Copies data and prints a success message upon completion.
        """

        # get BQ schema information
        plaid_cursors_bq = self.bq_tables.plaid_cursors_YYYYMMDD()
        partition_date = self.bq.get_partition_date(offset_days=offset_days)
        table_prefix = self.bq.replace_table_suffix(plaid_cursors_bq["table_id"])
        table_id = table_prefix + partition_date

        plaid_cursors_full_table = self.bq.concat_table_name(
            plaid_cursors_bq["project_id"], plaid_cursors_bq["dataset_id"], table_id
        )

        temp_cursors_bq = self.bq_tables.temp_plaid_cursors()
        temp_cursors_full_table = self.bq.concat_table_name(
            temp_cursors_bq["project_id"], temp_cursors_bq["dataset_id"], temp_cursors_bq["table_id"]
        )

        # Copy temp_cursors -> plaid_cursors_YYYYMMDD
        self.bq.copy_bq_table(
            source_table=temp_cursors_full_table,
            destination_table=plaid_cursors_full_table,
            write_disposition=write_disposition,
        )

    def create_removed_df(self, item_id, removed_transactions, removed_accounts, partition_date):
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

        # item_id and date_removed (partition_date) are singular values, create lists to match the number of removed transactions
        item_ids = [item_id] * len(removed_transactions)
        dates_removed = [partition_date] * len(removed_transactions)

        # add data to removed_df
        removed_df = pd.DataFrame(
            {
                "item_id": item_ids,
                "account_id": removed_accounts,
                "transaction_id": removed_transactions,
                "date_removed": dates_removed,
            }
        )

        # remove any duplicates, if any
        removed_df.drop_duplicates(inplace=True)

        return removed_df

    def create_transactions_df(self, transactions, item_id, status_type):
        """
        Create a DataFrame containing transaction data from get_transactions().

        Args:
            transactions (dict): A dictionary containing transaction information.
            status_type (str): The status type to be assigned to all transactions.

        Returns:
            pandas.DataFrame: A DataFrame containing transaction data
        """
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
                "item_id": item_ids,
                "account_id": account_ids,
                "transaction_id": transaction_ids,
                "pending_transaction_id": pending_transaction_ids,
                "is_pending": pendings,
                "account_owner": account_owners,
                "status": statuses,
                "date": dates,
                "datetime": datetimes,
                "authorized_date": authorized_dates,
                "authorized_datetime": authorized_datetimes,
                "amount": amounts,
                "currency_code": currency_codes,
                "unofficial_currency_code": unofficial_currency_codes,
                "personal_finance_category": [  # personal_finance_category struct with confidence_level, primary, and detailed fields
                    {
                        "primary": primary,
                        "detailed": detailed,
                        "confidence_level": confidence_level,
                    }
                    for primary, detailed, confidence_level in zip(
                        personal_finance_category_primaries,
                        personal_finance_category_detailed,
                        personal_finance_category_confidence_levels,
                    )
                ],
                "payment_channel": payment_channels,
                "merchant": [  # merchant struct with entity_id, merchant_name, and name fields
                    {
                        "entity_id": entity_id,
                        "merchant_name": merchant_name,
                        "name": name,
                        "website": website,
                    }
                    for entity_id, merchant_name, name, website in zip(
                        merchant_entity_ids, merchant_names, names, websites
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
                        addresses, cities, regions, postal_codes, latitudes, longitudes
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
                    for reference_number, ppd_id, payee, by_order_of, payer, payment_method, payment_processor, reason in zip(
                        reference_numbers,
                        ppd_ids,
                        payees,
                        by_order_ofs,
                        payers,
                        payment_methods,
                        payment_processors,
                        reasons,
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

    def create_empty_transactions_bq_table(self, offset_days, confirm):
        """
        Creates an empty plaid_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset_days (int): The number of days to offset when determining the partition date for the table.
            confirm (bool): If True and table already exists, will prompt user to delete the table. If False or table does not exist, table will automatically be created.

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """
        # get BQ schema information
        plaid_transactions_bq = self.bq_tables.plaid_transactions_YYYYMMDD()
        partition_date = self.bq.get_partition_date(offset_days=offset_days)
        table_prefix = self.bq.replace_table_suffix(plaid_transactions_bq["table_id"])
        table_id = table_prefix + partition_date

        # create empty table to store account data
        self.bq.create_empty_bq_table(
            project_id=plaid_transactions_bq["project_id"],
            dataset_id=plaid_transactions_bq["dataset_id"],
            table_id=table_id,
            table_description=plaid_transactions_bq["table_description"],
            table_schema=plaid_transactions_bq["table_schema"],
            confirm=confirm,
        )

    def create_empty_removed_bq_table(self, offset_days, confirm):
        """
        Creates an empty plaid_removed_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset_days (int): The number of days to offset when determining the partition date for the table.
            confirm (bool): If True and table already exists, will prompt user to delete the table. If False or table does not exist, table will automatically be created.

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """

        # get BQ schema information
        plaid_removed_bq = self.bq_tables.plaid_removed_transactions_YYYYMMDD()
        partition_date = self.bq.get_partition_date(offset_days=offset_days)
        table_prefix = self.bq.replace_table_suffix(plaid_removed_bq["table_id"])
        table_id = table_prefix + partition_date

        # create empty table to store account data
        self.bq.create_empty_bq_table(
            project_id=plaid_removed_bq["project_id"],
            dataset_id=plaid_removed_bq["dataset_id"],
            table_id=table_id,
            table_description=plaid_removed_bq["table_description"],
            table_schema=plaid_removed_bq["table_schema"],
            confirm=confirm,
        )

    def upload_transactions_df_to_bq(self, transactions_df):
        """
        Upload the transactions_df to a pre-existing plaid_transactions_YYYYMMDD BQ table

        Args:
            transactions_df (pandas.DataFrame): the dataframe containing all plaid transactions

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_transactions_bq = self.bq_tables.plaid_transactions_YYYYMMDD()

        full_table_name = self.bq.get_latest_full_table_name(
            plaid_transactions_bq["dataset_id"], plaid_transactions_bq["table_id"]
        )

        return self.bq_client.load_table_from_dataframe(transactions_df, full_table_name)

    def upload_removed_df_to_bq(self, removed_df):
        """
        Upload the removed_df to a pre-existing plaid_removed_transactions_YYYYMMDD BQ table

        Args:
            removed_df (pandas.DataFrame): the dataframe containing all plaid removed transactions

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_removed_bq = self.bq_tables.plaid_removed_transactions_YYYYMMDD()

        full_table_name = self.bq.get_latest_full_table_name(
            plaid_removed_bq["dataset_id"], plaid_removed_bq["table_id"]
        )

        return self.bq_client.load_table_from_dataframe(removed_df, full_table_name)
