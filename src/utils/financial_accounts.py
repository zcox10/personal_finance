import time
import pandas as pd
import sys
from google.api_core.exceptions import NotFound
from utils.bq_utils import BqUtils
from utils.crypto_utils import CryptoUtils
from jobs.bq_table_schemas import BqTableSchemas


class FinancialAccounts:
    def __init__(self, bq_client, plaid_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__crypto = CryptoUtils()
        self.__plaid_client = plaid_client
        self.__bq_tables = BqTableSchemas()

    def __create_crypto_accounts_df(self, eth_addresses, btc_addresses, eth_api_key, btc_api_key):
        item_ids = []
        persistent_account_ids = []
        account_ids = []
        account_masks = []
        account_names = []
        account_official_names = []
        account_types = []
        account_subtypes = []
        account_sources = []
        institution_ids = []
        institution_names = []
        balances = []
        update_types = []
        consent_expiration_times = []
        products = []
        billed_products = []

        crypto_balances = self.__crypto.get_crypto_balances(eth_addresses, btc_addresses, eth_api_key, btc_api_key)

        # if no results, return None
        if len(crypto_balances) == 0:
            return None

        # else, add data to df
        for k, v in crypto_balances.items():
            item_ids.append(k)
            persistent_account_ids.append(None)
            account_ids.append(None)
            account_masks.append(None)
            account_names.append(v["unofficial_currency_code"])
            account_official_names.append(v["unofficial_currency_code"])
            account_types.append("investment")
            account_subtypes.append("cryptocurrency")
            account_sources.append("CRYPTO")
            institution_ids.append(None)
            institution_names.append(None)
            balances.append(
                {
                    "available": v["available"],
                    "current": v["current"],
                    "limit": v["limit"],
                    "currency_code": v["currency_code"],
                    "unofficial_currency_code": v["unofficial_currency_code"],
                }
            )
            update_types.append(None)
            consent_expiration_times.append(None)
            products.append([])
            billed_products.append([])

        return pd.DataFrame(
            {
                "item_id": pd.Series(item_ids, dtype="str"),
                "persistent_account_id": pd.Series(persistent_account_ids, dtype="str"),
                "account_id": pd.Series(account_ids, dtype="str"),
                "account_mask": pd.Series(account_masks, dtype="str"),
                "account_name": pd.Series(account_names, dtype="str"),
                "account_official_name": pd.Series(account_official_names, dtype="str"),
                "account_type": pd.Series(account_types, dtype="str"),
                "account_subtype": pd.Series(account_subtypes, dtype="str"),
                "account_source": pd.Series(account_sources, dtype="str"),
                "institution_id": pd.Series(institution_ids, dtype="str"),
                "institution_name": pd.Series(institution_names, dtype="str"),
                "balance": balances,
                "update_type": pd.Series(update_types, dtype="str"),
                "consent_expiration_time": pd.Series(consent_expiration_times, dtype="datetime64[ns]"),
                "products": products,
                "billed_products": billed_products,
            }
        )

    def __create_plaid_accounts_df(self, access_token, plaid_country_codes):
        """
        Create a DataFrame containing Plaid financial account information.

        Args:
            access_token (str): The Plaid access token.
            plaid_country_codes (List[str]): List of country codes used for Plaid API requests.

        Returns:
            pandas.DataFrame: DataFrame containing financial account data
        """
        responses = self.__plaid_client.get_accounts(access_token)

        # only continue if data is present
        if len(responses["accounts"]) == 0:
            return None

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
        balances = []
        update_types = []
        consent_expiration_times = []
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

            # order balance dict in preferred order
            balances.append(
                {
                    "available": r["balances"]["available"],
                    "current": r["balances"]["current"],
                    "limit": r["balances"]["limit"],
                    "currency_code": r["balances"]["iso_currency_code"],
                    "unofficial_currency_code": r["balances"]["unofficial_currency_code"],
                }
            )

            # item data
            item_ids.append(responses["item"]["item_id"])
            institution_ids.append(responses["item"]["institution_id"])
            update_types.append(responses["item"]["update_type"])
            consent_expiration_times.append(responses["item"]["consent_expiration_time"])
            billed_products.append(responses["item"]["billed_products"])
            products.append(responses["item"]["products"])

        # get institution names
        distinct_institutions = list(set(institution_ids))

        institutions_dict = {}
        for institution_id in distinct_institutions:
            institution = self.__plaid_client.get_institution_by_id(institution_id, plaid_country_codes)
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
                "item_id": pd.Series(item_ids, dtype="str"),
                "persistent_account_id": pd.Series(persistent_account_ids, dtype="str"),
                "account_id": pd.Series(account_ids, dtype="str"),
                "account_mask": pd.Series(account_masks, dtype="str"),
                "account_name": pd.Series(account_names, dtype="str"),
                "account_official_name": pd.Series(account_official_names, dtype="str"),
                "account_type": pd.Series(account_types, dtype="str"),
                "account_subtype": pd.Series(account_subtypes, dtype="str"),
                "account_source": ["PLAID"] * len(item_ids),  # indicates account originates from Plaid
                "institution_id": pd.Series(institution_ids, dtype="str"),
                "institution_name": pd.Series(institution_names, dtype="str"),
                "balance": balances,
                "update_type": pd.Series(update_types, dtype="str"),
                "consent_expiration_time": pd.Series(consent_expiration_times, dtype="datetime64[ns]"),
                "products": products,
                "billed_products": billed_products,
            }
        )
        return accounts_df

    def __check_account_duplicates(self, full_table_name, accounts_df):
        """
        Check for duplicate accounts in the financial_accounts table.

        Args:
            full_table_name (str): The full name of the table in BigQuery.
            accounts_df (pandas.DataFrame): DataFrame containing account information.

        Returns:
            bool: True if there are no duplicates or the user decides to continue despite duplicates, False otherwise.
        """

        # determine if there are duplicate accounts in financial_accounts table
        try:
            # store persistent_account_id and account_id in get_accounts_df
            accts_q = f"""
            SELECT DISTINCT
              persistent_account_id,
              account_id
            FROM `{full_table_name}`
            """
            get_accts_df = self.__bq.query(accts_q)

            # determine if there are any duplicate persistent_account_id's
            get_persistent_account_ids = [i for i in get_accts_df["persistent_account_id"].unique() if i is not None]
            duplicate_persistent_account_ids = [
                i for i in accounts_df["persistent_account_id"] if i in get_persistent_account_ids
            ]

            # if there are duplicates, prompt user if they want to add the accounts anyways
            if len(duplicate_persistent_account_ids) > 0:
                print("persistsent_account_id(s) already present in the data:")
                for i in duplicate_persistent_account_ids:
                    print(f"- {i}")

                user_decision = self.__bq.user_prompt(
                    prompt="\nDo you want to continue?",
                    action_response="adding persistent_account_id(s) that are already present",
                    non_action_response="did not add persistent_account_id(s)",
                )
                if not user_decision:
                    return False

            # same process as above, determine if there are duplicates.
            # If there are duplicates, prompt user if they want to continue or not
            get_account_ids = [i for i in get_accts_df["account_id"].unique() if i is not None]
            duplicate_account_ids = [i for i in accounts_df["account_id"] if i in get_account_ids]
            if len(duplicate_account_ids) > 0:
                print("account_id(s) already present in the data:")
                for i in duplicate_account_ids:
                    print(f"- {i}")

                # prompt user to continue
                user_decision = self.__bq.user_prompt(
                    prompt="\nDo you want to continue?",
                    action_response="adding account_id(s) that are already present",
                    non_action_response="did not add account_id(s)",
                )
                if user_decision:
                    return True
                else:
                    return False

            return True

        except NotFound:
            print(f"`{full_table_name}` does not exist!")
            return False
        except Exception as e:
            print("\n" + str(e))
            sys.exit(1)

    def create_final_accounts_df(
        self, plaid_access_tokens, plaid_country_codes, eth_addresses, btc_addresses, eth_api_key, btc_api_key
    ):
        # list to store df's in for BQ upload
        df_list = []

        # add Plaid accounts
        for token in list(set(plaid_access_tokens)):
            accounts_df = self.__create_plaid_accounts_df(token, plaid_country_codes)
            if accounts_df is not None:
                df_list.append(accounts_df)

        crypto_df = self.__create_crypto_accounts_df(eth_addresses, btc_addresses, eth_api_key, btc_api_key)
        if crypto_df is not None:
            df_list.append(crypto_df)

        if len(df_list) == 0:
            return None
        else:
            df = pd.concat(df_list).reset_index(drop=True)
            # print(df["balance"])
            return df

    def add_plaid_accounts_to_bq(
        self,
        plaid_access_tokens,
        plaid_country_codes,
        eth_addresses,
        btc_addresses,
        eth_api_key,
        btc_api_key,
        offset,
        write_disposition,
    ):
        """
        Add accounts data to a defined BQ table.

        Args:
            access_token (str): Plaid access token.
            plaid_country_codes (list): Plaid country codes in list form.
            offset (int): The offset to be applied to a given partition date

        Returns:
            None
        """

        # generate schema for new financial_accounts_YYYYMMDD table
        financial_accounts_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.financial_accounts_YYYYMMDD(),
            offset=offset,
        )

        # create accounts_df to upload to BQ
        accounts_df = self.create_final_accounts_df(
            plaid_access_tokens, plaid_country_codes, eth_addresses, btc_addresses, eth_api_key, btc_api_key
        )
        if accounts_df is not None:
            self.__bq.load_df_to_bq(
                accounts_df,
                financial_accounts_bq["full_table_name"],
                financial_accounts_bq["table_schema"],
                write_disposition,
            )
            print(f"SUCCESS: all access tokens added to `{financial_accounts_bq['full_table_name']}`\n")
        else:
            print("No accounts present in accounts_df")

    def create_empty_accounts_bq_table(self, offset, write_disposition):
        """
        Creates an empty plaid_transactions_YYYYMMDD table in BQ for a specific partition date.

        Args:
            offset (int): The offset to be applied to a given partition date
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None: This function does not return anything. Prints table details or a success message upon completion.
        """
        # get BQ schema information
        financial_accounts_bq = self.__bq.update_table_schema_partition(
            schema=self.__bq_tables.financial_accounts_YYYYMMDD(), offset=offset
        )

        # create empty table to store account data
        self.__bq.create_empty_bq_table(
            project_id=financial_accounts_bq["project_id"],
            dataset_id=financial_accounts_bq["dataset_id"],
            table_id=financial_accounts_bq["table_id"],
            table_description=financial_accounts_bq["table_description"],
            table_schema=financial_accounts_bq["table_schema"],
            write_disposition=write_disposition,
        )
