import json
import pandas as pd
from google.api_core.exceptions import NotFound

from utils.bq_utils import BqUtils
from sql.bq_table_schemas import BqTableSchemas


class PlaidAccounts:
    def __init__(self, bq_client, plaid_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__plaid_client = plaid_client
        self.__bq_tables = BqTableSchemas()

    def __create_accounts_df(self, access_token, plaid_country_codes):
        responses = self.__plaid_client.get_accounts(access_token)

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
        return accounts_df

    def __check_account_duplicates(self, full_table_name, accounts_df):
        # determine if there are duplicate accounts in plaid_accounts table
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
            print(e)
            return False

    def add_accounts_to_bq(self, access_token, plaid_country_codes):
        """
        Add accounts data to a defined BQ table.

        Args:
            access_token (str): Plaid access token.
            plaid_country_codes (list): Plaid country codes in list form.

        Returns:
            None
        """

        accounts_df = self.__create_accounts_df(access_token, plaid_country_codes)

        # Define your BigQuery table reference
        plaid_accounts_bq = self.__bq_tables.plaid_accounts()

        # (bool): if true, there are no duplicates and continue OR there are duplicates but chose to continue anyways
        decision = self.__check_account_duplicates(plaid_accounts_bq["full_table_name"], accounts_df)
        if decision:
            # Load the record to cursors temp BQ table. "WRITE_APPEND" because there are multiple individual uploads
            self.__bq.load_df_to_bq(accounts_df, plaid_accounts_bq["full_table_name"], "WRITE_APPEND")

    def create_accounts_bq_table(self, access_tokens, plaid_country_codes, write_disposition):
        """
        Creates an empty BigQuery table to store Plaid account data. Uses the provided access tokens
        to fetch account information from the Plaid API and adds it to the BigQuery table.

        Args:
            access_tokens (list): A list of access tokens for accessing Plaid API and fetching account data.
            plaid_country_codes (list): A list of Plaid country codes to specify the country for which accounts are fetched.
            write_disposition (str): Options include WRITE_TRUNCTE, WRITE_APPEND, and WRITE_EMPTY

        Returns:
            None
        """

        # get BQ schema information
        plaid_accounts_bq = self.__bq_tables.plaid_accounts()

        # create empty table to store account data
        self.__bq.create_empty_bq_table(
            project_id=plaid_accounts_bq["project_id"],
            dataset_id=plaid_accounts_bq["dataset_id"],
            table_id=plaid_accounts_bq["table_id"],
            table_description=plaid_accounts_bq["table_description"],
            table_schema=plaid_accounts_bq["table_schema"],
            write_disposition=write_disposition,
        )

        # add access tokens to new empty accounts table
        for token in access_tokens:
            self.add_accounts_to_bq(
                access_token=token,
                plaid_country_codes=plaid_country_codes,
            )

        print(f"SUCCESS: all access tokens added to `{plaid_accounts_bq['full_table_name']}`\n")
