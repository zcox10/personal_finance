import pandas as pd
from sql.bq_table_schemas import BqTableSchemas
from utils.bq_utils import BqUtils


class PlaidInvestments:
    def __init__(self, bq_client, plaid_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__plaid_client = plaid_client
        self.__bq_tables = BqTableSchemas()

    def get_investments_final(self, access_token):
        investments_json = self.__get_investments(access_token)
        holdings_df = self.__create_holdings_df(investments_json["holdings"])
        securities_df = self.__create_securities_df(investments_json["securities"])

        # TODO: upload df's to bq here

        return holdings_df, securities_df

    def __get_investments(self, access_token):
        response = self.__plaid_client.get_investment_holdings(access_token)
        return response

    def __create_holdings_df(self, investments_json):
        # holdings
        account_ids = []
        cost_basis = []
        institution_prices = []
        institution_price_dates = []
        institution_price_datetimes = []
        institution_values = []
        currency_codes = []
        unofficial_currency_codes = []
        quantities = []
        security_ids = []
        vested_quantities = []
        vested_values = []

        for i in investments_json:
            account_ids.append(i["account_id"])
            cost_basis.append(i["cost_basis"])
            institution_prices.append(i["institution_price"])
            institution_price_dates.append(i["institution_price_as_of"])
            institution_price_datetimes.append(i["institution_price_datetime"])
            institution_values.append(i["institution_value"])
            currency_codes.append(i["iso_currency_code"])
            unofficial_currency_codes.append(i["unofficial_currency_code"])
            quantities.append(i["quantity"])
            security_ids.append(i["security_id"])
            vested_quantities.append(i["vested_quantity"])
            vested_values.append(i["vested_value"])

        holdings_df = pd.DataFrame(
            {
                "account_id": account_ids,
                "cost_basis": cost_basis,
                "institution_price": institution_prices,
                "institution_price_date": institution_price_dates,
                "institution_price_datetime": institution_price_datetimes,
                "institution_value": institution_values,
                "currency_code": currency_codes,
                "unofficial_currency_code": unofficial_currency_codes,
                "quantity": quantities,
                "security_id": security_ids,
                "vested_quantity": vested_quantities,
                "vested_value": vested_values,
            }
        )
        return holdings_df

    def __create_securities_df(self, investments_json):
        # securities
        close_prices = []
        close_price_dates = []
        update_datetimes = []
        cusip = []
        institution_ids = []
        institution_security_ids = []
        is_cash_equivalents = []
        isin = []
        currency_codes = []
        unofficial_currency_codes = []
        market_identifier_codes = []
        names = []
        option_contracts = []
        proxy_security_ids = []
        security_ids = []
        sedol = []
        ticker_symbols = []
        types = []

        for i in investments_json:
            close_prices.append(i["close_price"])
            close_price_dates.append(i["close_price_as_of"])
            update_datetimes.append(i["update_datetime"])
            cusip.append(i["cusip"])
            institution_ids.append(i["institution_id"])
            institution_security_ids.append(i["institution_security_id"])
            is_cash_equivalents.append(i["is_cash_equivalent"])
            isin.append(i["isin"])
            currency_codes.append(i["iso_currency_code"])
            unofficial_currency_codes.append(i["unofficial_currency_code"])
            market_identifier_codes.append(i["market_identifier_code"])
            names.append(i["name"])
            option_contracts.append(i["option_contract"])
            proxy_security_ids.append(i["proxy_security_id"])
            security_ids.append(i["security_id"])
            sedol.append(i["sedol"])
            ticker_symbols.append(i["ticker_symbol"])
            types.append(i["type"])

        securities_df = pd.DataFrame(
            {
                "close_price": close_prices,
                "close_price_date": close_price_dates,
                "update_datetime": update_datetimes,
                "cusip": cusip,
                "institution_id": institution_ids,
                "institution_security_id": institution_security_ids,
                "is_cash_equivalent": is_cash_equivalents,
                "isin": isin,
                "currency_code": currency_codes,
                "unofficial_currency_code": unofficial_currency_codes,
                "market_identifier_code": market_identifier_codes,
                "name": names,
                "option_contract": option_contracts,
                "proxy_security_id": proxy_security_ids,
                "security_id": security_ids,
                "sedol": sedol,
                "ticker_symbol": ticker_symbols,
                "type": types,
            }
        )
        return securities_df
