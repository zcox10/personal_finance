import pandas as pd
import time
from schemas.bq_table_schemas import BqTableSchemas
from utils.bq_utils import BqUtils


class PlaidInvestments:
    def __init__(self, bq_client, plaid_client):
        self._bq = BqUtils(bq_client=bq_client)
        self._plaid_client = plaid_client
        self._bq_tables = BqTableSchemas()

    def generate_investments_dfs(self, start_date, end_date, access_token):
        """
        Generate DataFrames for investment holdings and transactions.

        Args:
            start_date (str): The start date for investment transactions data.
            end_date (str): The end date for investment transactions data.
            access_token (str): The Plaid access token.

        Returns:
            tuple: A tuple containing two DataFrames:
                - holdings_df (pandas.DataFrame): DataFrame containing investment holdings data.
                - investment_transactions_df (pandas.DataFrame): DataFrame containing investment transactions data.
        """

        investments_holdings_json = self._plaid_client.get_investment_holdings_data(access_token)
        holdings_df = self._create_holdings_df(investments_holdings_json)

        investment_transactions_json, securities_json, item_id = (
            self._plaid_client.get_investment_transactions_data(start_date, end_date, access_token)
        )

        investment_transactions_df = self._create_investment_transactions_df(
            investment_transactions_json, securities_json, item_id
        )

        return holdings_df, investment_transactions_df

    def generate_investments_dfs_list(self, start_date, end_date, access_tokens):
        """
        Generate lists of DataFrames for investment holdings and transactions for multiple access tokens.

        Args:
            start_date (str): The start date for investment transactions data.
            end_date (str): The end date for investment transactions data.
            access_tokens (List[str]): List of Plaid access tokens.

        Returns:
            tuple: A tuple containing two lists of DataFrames:
                - holdings_df_list (List[pandas.DataFrame]): List of DataFrames containing investment holdings data.
                - investment_transactions_df_list (List[pandas.DataFrame]): List of DataFrames containing investment transactions data.
        """

        holdings_df_list = []
        investment_transactions_df_list = []
        for token in access_tokens:
            holdings_df, investment_transactions_df = self.generate_investments_dfs(
                start_date, end_date, token
            )

            if holdings_df is not None:
                holdings_df_list.append(holdings_df)

            if investment_transactions_df is not None:
                investment_transactions_df_list.append(investment_transactions_df)

        return holdings_df_list, investment_transactions_df_list

    def _create_holdings_df(self, investments_json):
        """
        Create a DataFrame containing investment holdings data.

        Args:
            investments_json (dict): JSON data containing investment holdings information.

        Returns:
            pandas.DataFrame: DataFrame containing the following columns:
        """
        # return None if there is no data available
        if len(investments_json["holdings"]) == 0:
            return None

        # holdings
        item_ids = []
        account_ids = []
        cost_basis = []
        institution_prices = []
        institution_price_dates = []
        institution_price_datetimes = []
        institution_values = []
        currency_codes = []
        unofficial_currency_codes = []
        quantities = []
        vested_quantities = []
        vested_values = []
        security_ids = []

        for i in investments_json["holdings"]:
            item_ids.append(investments_json["item"]["item_id"])
            account_ids.append(i["account_id"])
            cost_basis.append(i["cost_basis"])
            institution_prices.append(i["institution_price"])
            institution_price_dates.append(i["institution_price_as_of"])
            institution_price_datetimes.append(i["institution_price_datetime"])
            institution_values.append(i["institution_value"])
            currency_codes.append(i["iso_currency_code"])
            unofficial_currency_codes.append(i["unofficial_currency_code"])
            quantities.append(i["quantity"])
            vested_quantities.append(i["vested_quantity"])
            vested_values.append(i["vested_value"])
            security_ids.append(i["security_id"])

        securities_data = self._create_securities_dict(investments_json["securities"], security_ids)

        holdings_df = pd.DataFrame(
            {
                "item_id": pd.Series(item_ids, dtype="str"),
                "account_id": pd.Series(account_ids, dtype="str"),
                "cost_basis": pd.Series(cost_basis, dtype="float64"),
                "institution_price": pd.Series(institution_prices, dtype="float64"),
                "institution_price_date": pd.Series(
                    institution_price_dates, dtype="datetime64[ns]"
                ),
                "institution_price_datetime": pd.Series(
                    institution_price_datetimes, dtype="datetime64[ns]"
                ),
                "institution_value": pd.Series(institution_values, dtype="float64"),
                "currency_code": pd.Series(currency_codes, dtype="str"),
                "unofficial_currency_code": pd.Series(unofficial_currency_codes, dtype="str"),
                "quantity": pd.Series(quantities, dtype="float64"),
                "vested_quantity": pd.Series(vested_quantities, dtype="float64"),
                "vested_value": pd.Series(vested_values, dtype="float64"),
                "security": securities_data,
            }
        )
        return holdings_df

    def _create_investment_transactions_df(self, investments_json, securities_json, item_id):
        """
        Create a DataFrame containing investment transactions data.

        Args:
            investments_json (dict): JSON data containing investment transactions information.
            securities_json (dict): JSON data containing securities information.
            item_id (str): The Plaid item ID.

        Returns:
            pandas.DataFrame: DataFrame containing the following columns:
        """

        # return None if there is no data available
        if len(investments_json["investment_transactions"]) == 0:
            return None

        # holdings
        item_ids = []
        account_ids = []
        investment_transaction_ids = []
        dates = []
        names = []
        quantities = []
        amounts = []
        prices = []
        fees = []
        types = []
        subtypes = []
        currency_codes = []
        unofficial_currency_codes = []
        security_ids = []

        # print(investments_json["investment_transactions"])

        for i in investments_json["investment_transactions"]:
            item_ids.append(item_id)
            account_ids.append(i["account_id"])
            investment_transaction_ids.append(i["investment_transaction_id"])
            dates.append(i["date"])
            names.append(i["name"])
            quantities.append(i["quantity"])
            amounts.append(i["amount"])
            prices.append(i["price"])
            fees.append(i["fees"])
            types.append(i["type"])
            subtypes.append(i["subtype"])
            currency_codes.append(i["iso_currency_code"])
            unofficial_currency_codes.append(i["unofficial_currency_code"])
            security_ids.append(i["security_id"])

        securities_data = self._create_securities_dict(securities_json, security_ids)

        investment_transactions_df = pd.DataFrame(
            {
                "item_id": pd.Series(item_ids, dtype="str"),
                "account_id": pd.Series(account_ids, dtype="str"),
                "investment_transaction_id": pd.Series(investment_transaction_ids, dtype="str"),
                "investment_date": pd.Series(dates, dtype="datetime64[ns]"),
                "investment_name": pd.Series(names, dtype="str"),
                "quantity": pd.Series(quantities, dtype="float64"),
                "amount": pd.Series(amounts, dtype="float64"),
                "price": pd.Series(prices, dtype="float64"),
                "fees": pd.Series(fees, dtype="float64"),
                "investment_type": pd.Series(types, dtype="str"),
                "investment_subtype": pd.Series(subtypes, dtype="str"),
                "currency_code": pd.Series(currency_codes, dtype="str"),
                "unofficial_currency_code": pd.Series(unofficial_currency_codes, dtype="str"),
                "security": securities_data,
            }
        )
        return investment_transactions_df

    def _create_securities_dict(self, securities_json, security_ids):
        """
        Create a dictionary containing securities information.

        Args:
            securities_json (dict): JSON data containing securities information.
            security_ids (List[str]): List of security IDs to include in the dictionary.

        Returns:
            List[dict]: List of dictionaries containing securities data.
        """

        # generate individual securities dict's, then store them in securities_data list
        securities_dict = {}
        for j in securities_json:
            if j["option_contract"] is None:
                option_contract = {
                    "contract_type": None,
                    "expiration_date": None,
                    "strike_price": None,
                    "underlying_security_ticker": None,
                }
            else:
                option_contract = {
                    "contract_type": j["option_contract"]["contract_type"],
                    "expiration_date": j["option_contract"]["expiration_date"],
                    "strike_price": j["option_contract"]["strike_price"],
                    "underlying_security_ticker": j["option_contract"][
                        "underlying_security_ticker"
                    ],
                }

            securities_dict[j["security_id"]] = {
                "security_id": j["security_id"],
                "currency_code": j["iso_currency_code"],
                "unofficial_currency_code": j["unofficial_currency_code"],
                "close_price": j["close_price"],
                "close_price_date": j["close_price_as_of"],
                "update_datetime": j["update_datetime"],
                "cusip": j["cusip"],
                "institution_id": j["institution_id"],
                "institution_security_id": j["institution_security_id"],
                "is_cash_equivalent": j["is_cash_equivalent"],
                "isin": j["isin"],
                "market_identifier_code": j["market_identifier_code"],
                "name": j["name"],
                "option_contract": option_contract,
                "proxy_security_id": j["proxy_security_id"],
                "sedol": j["sedol"],
                "ticker_symbol": j["ticker_symbol"],
                "type": j["type"],
            }

        # final securities_data list to return for insertion to df
        securities_data = []
        for security_id in security_ids:
            if security_id is None:
                securities_data.append(self._empty_securities_dict())
            else:
                securities_data.append(securities_dict[security_id])

        return securities_data

    def _empty_securities_dict(self):
        """
        Create an empty dictionary representing securities data.

        Returns:
            dict: An empty dictionary with keys corresponding to securities data fields.
        """
        return {
            "security_id": None,
            "currency_code": None,
            "unofficial_currency_code": None,
            "close_price": None,
            "close_price_date": None,
            "update_datetime": None,
            "cusip": None,
            "institution_id": None,
            "institution_security_id": None,
            "is_cash_equivalent": None,
            "isin": None,
            "market_identifier_code": None,
            "name": None,
            "option_contract": {
                "contract_type": None,
                "expiration_date": None,
                "strike_price": None,
                "underlying_security_ticker": None,
            },
            "proxy_security_id": None,
            "sedol": None,
            "ticker_symbol": None,
            "type": None,
        }

    def upload_investment_transactions_df_to_bq(self, investment_transactions_df, offset):
        """
        Upload the transactions_df to a pre-existing plaid_transactions_YYYYMMDD BQ table

        Args:
            investment_transactions_df (pandas.DataFrame): the dataframe containing all plaid transactions
            offset (int): The offset to be applied to a given partition date

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_investment_transactions_bq = self._bq.update_table_schema_partition(
            self._bq_tables.plaid_investment_transactions_YYYYMMDD(),
            offset=offset,
        )

        # upload df to plaid_transactions_YYYYMMDD. "WRITE_APPEND" because multiple transaction_df's will be loaded
        return self._bq.load_df_to_bq(
            investment_transactions_df,
            plaid_investment_transactions_bq.full_table_name,
            plaid_investment_transactions_bq.table_schema,
            "WRITE_APPEND",
        )

    def upload_investment_transactions_df_list_to_bq(
        self, investment_transactions_df_list, offset, write_disposition
    ):
        """
        Upload a list of investment transactions DataFrames to BigQuery.

        Args:
            investment_transactions_df_list (List[pandas.DataFrame]): List of DataFrames containing investment transactions data.
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Write disposition for BigQuery ("WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY").

        Returns:
            None
        """

        # only upload investment_transactions_df to BQ if there is at least one non-null df
        if len(investment_transactions_df_list) == 0:
            print("No investment transactions present\n")
        else:
            concat_investment_transactions_df = pd.concat(investment_transactions_df_list)
            self.upload_investment_transactions_df_to_bq(concat_investment_transactions_df, offset)

    def upload_investment_holdings_df_to_bq(self, holdings_df, offset):
        """
        Upload the holdings_df to a pre-existing plaid_investment_holdings_YYYYMMDD BQ table

        Args:
            holdings_df (pandas.DataFrame): the dataframe containing all plaid removed transactions
            offset (int): The offset to be applied to a given partition date

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_investment_holdings_bq = self._bq.update_table_schema_partition(
            self._bq_tables.plaid_investment_holdings_YYYYMMDD(),
            offset=offset,
        )

        # upload df to plaid_removed_transactions_YYYYMMDD. "WRITE_APPEND" because multiple transaction_df's will be loaded
        return self._bq.load_df_to_bq(
            holdings_df,
            plaid_investment_holdings_bq.full_table_name,
            plaid_investment_holdings_bq.table_schema,
            "WRITE_APPEND",
        )

    def upload_investment_holdings_df_list_to_bq(self, holdings_df_list, offset, write_disposition):
        """
        Upload a list of investment holdings DataFrames to BigQuery.

        Args:
            holdings_df_list (List[pandas.DataFrame]): List of DataFrames containing investment holdings data.
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Write disposition for BigQuery ("WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY").

        Returns:
            None
        """

        # only upload holdings_df to BQ if there is at least one non-null df
        if len(holdings_df_list) == 0:
            print("No investment holdings present\n")
        else:
            concat_holdings_df = pd.concat(holdings_df_list)

            # upload holdings df to BQ
            self.upload_investment_holdings_df_to_bq(concat_holdings_df, offset)
