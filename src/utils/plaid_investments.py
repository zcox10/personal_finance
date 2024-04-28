import pandas as pd
from sql.bq_table_schemas import BqTableSchemas
from utils.bq_utils import BqUtils


class PlaidInvestments:
    def __init__(self, bq_client, plaid_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__plaid_client = plaid_client
        self.__bq_tables = BqTableSchemas()

    def generate_investments_dfs(self, start_date, end_date, access_token):
        investments_holdings_json = self.__plaid_client.get_investment_holdings_data(access_token)
        holdings_df = self.__create_holdings_df(investments_holdings_json)

        investment_transactions_json, securities_json, item_id = self.__plaid_client.get_investment_transactions_data(
            start_date, end_date, access_token
        )

        investment_transactions_df = self.__create_investment_transactions_df(
            investment_transactions_json, securities_json, item_id
        )

        return holdings_df, investment_transactions_df

    def __create_holdings_df(self, investments_json):
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

        securities_data = self.__create_securities_dict(investments_json["securities"], security_ids)

        holdings_df = pd.DataFrame(
            {
                "item_id": item_ids,
                "account_id": account_ids,
                "cost_basis": cost_basis,
                "institution_price": institution_prices,
                "institution_price_date": institution_price_dates,
                "institution_price_datetime": institution_price_datetimes,
                "institution_value": institution_values,
                "currency_code": currency_codes,
                "unofficial_currency_code": unofficial_currency_codes,
                "quantity": quantities,
                "vested_quantity": vested_quantities,
                "vested_value": vested_values,
                "security": securities_data,
            }
        )
        return holdings_df

    def __create_investment_transactions_df(self, investments_json, securities_json, item_id):
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

        securities_data = self.__create_securities_dict(securities_json, security_ids)

        investment_transactions_df = pd.DataFrame(
            {
                "item_id": item_ids,
                "account_id": account_ids,
                "investment_transaction_id": investment_transaction_ids,
                "date": dates,
                "name": names,
                "quantity": quantities,
                "amount": amounts,
                "price": prices,
                "fees": fees,
                "type": pd.Series(types, dtype="str"),  # ensure cast to str
                "subtype": pd.Series(subtypes, dtype="str"),
                "currency_code": currency_codes,
                "unofficial_currency_code": unofficial_currency_codes,
                "security": securities_data,
            }
        )
        return investment_transactions_df

    def __create_securities_dict(self, securities_json, security_ids):
        securities_dict = {}
        securities_data = []

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
                    "underlying_security_ticker": j["option_contract"]["underlying_security_ticker"],
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

        for security_id in security_ids:
            securities_data.append(securities_dict[security_id])

        return securities_data

    def create_empty_investment_transactions_bq_table(self, offset_days, write_disposition):
        """
        Creates an empty plaid_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset_days (int): The number of days to offset when determining the partition date for the table.
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """
        # get BQ schema information
        plaid_investment_transactions_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.plaid_investment_transactions_YYYYMMDD(), offset_days=offset_days
        )

        # create empty table to store account data
        self.__bq.create_empty_bq_table(
            project_id=plaid_investment_transactions_bq["project_id"],
            dataset_id=plaid_investment_transactions_bq["dataset_id"],
            table_id=plaid_investment_transactions_bq["table_id"],
            table_description=plaid_investment_transactions_bq["table_description"],
            table_schema=plaid_investment_transactions_bq["table_schema"],
            write_disposition=write_disposition,
        )

    def create_empty_investment_holdings_bq_table(self, offset_days, write_disposition):
        """
        Creates an empty plaid_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset_days (int): The number of days to offset when determining the partition date for the table.
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """
        # get BQ schema information
        plaid_investment_holdings_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.plaid_investment_holdings_YYYYMMDD(), offset_days=offset_days
        )

        # create empty table to store account data
        self.__bq.create_empty_bq_table(
            project_id=plaid_investment_holdings_bq["project_id"],
            dataset_id=plaid_investment_holdings_bq["dataset_id"],
            table_id=plaid_investment_holdings_bq["table_id"],
            table_description=plaid_investment_holdings_bq["table_description"],
            table_schema=plaid_investment_holdings_bq["table_schema"],
            write_disposition=write_disposition,
        )

    def upload_investment_transactions_df_to_bq(self, investment_transactions_df, offset_days):
        """
        Upload the transactions_df to a pre-existing plaid_transactions_YYYYMMDD BQ table

        Args:
            transactions_df (pandas.DataFrame): the dataframe containing all plaid transactions

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_investment_transactions_bq = self.__bq.update_table_schema_partition(
            self.__bq_tables.plaid_investment_transactions_YYYYMMDD(),
            offset_days=offset_days,
        )

        # upload df to plaid_transactions_YYYYMMDD. "WRITE_APPEND" because multiple transaction_df's will be loaded
        return self.__bq.load_df_to_bq(
            investment_transactions_df, plaid_investment_transactions_bq["full_table_name"], "WRITE_APPEND"
        )

    def upload_investment_holdings_df_to_bq(self, holdings_df, offset_days):
        """
        Upload the removed_df to a pre-existing plaid_removed_transactions_YYYYMMDD BQ table

        Args:
            removed_df (pandas.DataFrame): the dataframe containing all plaid removed transactions

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        plaid_investment_holdings_bq = self.__bq.update_table_schema_partition(
            self.__bq_tables.plaid_investment_holdings_YYYYMMDD(),
            offset_days=offset_days,
        )

        # upload df to plaid_removed_transactions_YYYYMMDD. "WRITE_APPEND" because multiple transaction_df's will be loaded
        return self.__bq.load_df_to_bq(holdings_df, plaid_investment_holdings_bq["full_table_name"], "WRITE_APPEND")
