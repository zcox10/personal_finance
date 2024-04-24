import json
from utils.bq_utils import BqUtils
from sql.bq_table_schemas import BqTableSchemas

from plaid.exceptions import ApiException
from plaid.configuration import Configuration
from plaid.api_client import ApiClient
from plaid.exceptions import ApiException
from plaid.api import plaid_api
from plaid.model.country_code import CountryCode
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest


class PlaidUtils:
    def __init__(self, bq_client, client_id, client_secret, host):
        self.__bq = BqUtils(bq_client=bq_client)
        self.plaid_client = self.authenticate(client_id, client_secret, host)
        self.__bq_tables = BqTableSchemas()

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

    def get_access_tokens(self, products=[]):
        """
        Gather all Plaid access tokens and items into a df. Used in create_cursors_bq_table()

        Args:
            product_filters (list): A list of products to filter for (e.g. transactions, investments)

        Returns:
            pandas.DataFrame: Details of the items.
        """

        # get BQ schema for plaid_accounts table
        plaid_accounts_bq = self.__bq_tables.plaid_accounts()

        if not self.__bq.does_bq_table_exist(
            plaid_accounts_bq["project_id"], plaid_accounts_bq["dataset_id"], plaid_accounts_bq["table_id"]
        ):
            print(f"`{plaid_accounts_bq['full_table_name']}` does not exist!")
            return None

        if len(products) != 0:
            products_filter = '"' + '", "'.join(products) + '"'
            where_clause = f"WHERE EXISTS(SELECT 1 FROM UNNEST(products) p WHERE p IN ({products_filter}))"
        else:
            where_clause = ""

        # generate query to pull access_token and item_id, then return as a df
        query = f"""
        SELECT DISTINCT
          access_token,
          item_id
        FROM `{plaid_accounts_bq["full_table_name"]}`
        {where_clause}
        """

        return self.__bq.query(query)

    def get_transactions_data(self, access_token, next_cursor):
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
        transactions_added = []
        transactions_modified = []

        while has_more:
            # Fetch transactions for the account
            request = TransactionsSyncRequest(access_token=access_token, cursor=next_cursor, count=500)

            response = self.plaid_client.transactions_sync(request)
            has_more = response["has_more"]
            next_cursor = response["next_cursor"]

            transactions_json = response.to_dict()

            # self.__bq.pretty_print_response(transactions_json)

            if len(transactions_json["added"]) > 0:
                transactions_added.append(transactions_json["added"])

            if len(transactions_json["modified"]) > 0:
                transactions_modified.append(transactions_json["modified"])

            # add all removed transactions to removed_transactions list
            if len(transactions_json["removed"]) > 0:
                for r in transactions_json["removed"]:
                    removed_accounts.append(r["account_id"])
                    removed_transactions.append(r["transaction_id"])

            # add test transaction id
            # removed_transactions.append("test_transaction_id")
            # removed_accounts.append("test_account_id")

        transactions_final = {"added": transactions_added, "modified": transactions_modified}
        return removed_transactions, removed_accounts, transactions_final

    def get_investment_holdings(self, access_token):
        # Pull Holdings for an Item
        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = self.plaid_client.investments_holdings_get(request)

        # # Handle Holdings response
        # holdings = response["holdings"]

        # # Handle Securities response
        # securities = response["securities"]

        return response
