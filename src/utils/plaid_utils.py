import json
from datetime import date
import pandas as pd
import logging
from utils.bq_utils import BqUtils
from jobs.bq_table_schemas import BqTableSchemas
from plaid.exceptions import ApiException
from plaid.configuration import Configuration
from plaid.api_client import ApiClient
from plaid.api import plaid_api
from plaid.model.country_code import CountryCode
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions
from plaid.model.item_remove_request import ItemRemoveRequest

# from plaid.model.link_token_transactions import LinkTokenTransactions


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
            logging.error("\n" + str(e))
            return e

    def get_item(self, access_token):
        """
        Get item associated with a Plaid access token.

        Args:
            access_token (str): Access token for Plaid API authentication.

        Returns:
            dict: Details of the accounts.
        """

        try:
            request = ItemGetRequest(access_token=access_token)
            response = self.plaid_client.item_get(request)
            return response.to_dict()

        except ApiException as e:
            logging.error("\n" + str(e))
            return e

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
            logging.error("\n" + str(e))
            return e

    def get_items_by_access_token(self, access_tokens, products=[]):
        """
        Gather all Plaid access tokens and items into a df. Used in create_cursors_bq_table()

        Args:
            access_tokens (list): A list of access_tokens to check for
            products (list): A list of products to filter for (e.g. transactions, investments)

        Returns:
            pandas.DataFrame: Details of the items.
        """

        item_ids = []
        item_access_tokens = []
        for token in access_tokens:
            item_dict = self.get_item(token)
            item_products = item_dict["item"]["products"]
            item_id = item_dict["item"]["item_id"]

            has_product = False
            for p in products:
                if p in item_products:
                    has_product = True

            if has_product:
                item_ids.append(item_id)
                item_access_tokens.append(token)

        return pd.DataFrame(
            {
                "item_id": pd.Series(item_ids, dtype="str"),
                "access_token": pd.Series(item_access_tokens, dtype="str"),
            }
        )

    def remove_item(self, access_token):
        try:
            request = ItemRemoveRequest(access_token=access_token)
            response = self.plaid_client.item_remove(request)
            return response

        except ApiException as e:
            logging.error("\n" + str(e))
            return e

    def get_transactions_data(self, access_token, next_cursor, add_test_transaction):
        """
        Retrieves transactions and handles pagination.

        Retrieves transactions using the provided access token and handles pagination if necessary.

        Args:
            access_token (str): Access token for Plaid API.
            item_id (str): ID of the item associated with the transactions.
            next_cursor (str): Cursor for fetching the next set of transactions.
            add_test_transaction (bool): True if should add a test transaction

        Returns:
            tuple: A tuple containing:
                - pandas.DataFrame: A DataFrame containing transaction data.
                - set: A set containing transaction IDs of removed transactions.

        Raises:
            ApiException: If an error occurs during the API request.
        """

        # get transactions
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
                transactions_added += transactions_json["added"]

            if len(transactions_json["modified"]) > 0:
                transactions_modified += transactions_json["modified"]

            # add all removed transactions to removed_transactions list
            if len(transactions_json["removed"]) > 0:
                for r in transactions_json["removed"]:
                    removed_accounts.append(r["account_id"])
                    removed_transactions.append(r["transaction_id"])

            # add test transaction id
            if add_test_transaction:
                removed_accounts.append("test_account_id")
                removed_transactions.append("test_transaction_id")

        transactions_final = {"added": transactions_added, "modified": transactions_modified}
        return removed_transactions, removed_accounts, transactions_final, next_cursor

    def get_investment_holdings_data(self, access_token):
        # Pull Holdings for an Item
        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = self.plaid_client.investments_holdings_get(request)

        return response

    def get_investment_transactions_data(self, start_date, end_date, access_token):
        RESULTS_COUNT = 500
        request = InvestmentsTransactionsGetRequest(
            access_token=access_token,
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
            options=InvestmentsTransactionsGetRequestOptions(count=RESULTS_COUNT),
        )
        response = self.plaid_client.investments_transactions_get(request)

        securities_json = response["securities"]
        item_id = response["item"]["item_id"]
        current_transactions_count = len(response["investment_transactions"])
        total_transactions_count = response["total_investment_transactions"]

        if current_transactions_count >= total_transactions_count:
            return response, securities_json, item_id
        else:
            investment_transactions_json = response["investment_transactions"]

            while current_transactions_count < total_transactions_count:
                request = InvestmentsTransactionsGetRequest(
                    access_token=access_token,
                    start_date=date.fromisoformat(start_date),
                    end_date=date.fromisoformat(end_date),
                    options=InvestmentsTransactionsGetRequestOptions(
                        count=RESULTS_COUNT, offset=current_transactions_count
                    ),
                )
                response = self.plaid_client.investments_transactions_get(request)
                investment_transactions_json += response["investment_transactions"]
                current_transactions_count += len(response["investment_transactions"])

            return investment_transactions_json, securities_json, item_id
