import pandas as pd
from google.cloud import bigquery
import plaid
from utils.bq_utils import BqUtils
from utils.plaid_utils import PlaidUtils
from sql.bq_table_schemas import BqTableSchemas

PLAID_CLIENT_ID = "65975384ab670e001c0aaf0d"
# PLAID_SECRET="56e33c77237c8c9e45f5c066b8b2fa" #production
PLAID_SECRET = "9294dd5ca4a5c99d90da56640f40e5"  # sandbox
PLAID_ENV = "sandbox"
PLAID_PRODUCTS = ["liabilities", "transactions", "investments"]
PLAID_COUNTRY_CODES = ["US"]
PLAID_HOST = plaid.Environment.Sandbox
# PLAID_REDIRECT_URI="https://localhost:3000/"
PLAID_ACCESS_TOKENS = [
    "access-sandbox-1902b6d6-33f3-49b8-a3ce-3213a5fe9a49",
    "access-sandbox-12481aea-33b6-4101-9f49-4e07ebbc1431",
    "access-sandbox-f415798a-74af-4e7f-b504-dc075dc5889c",
]


bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
bq_tables = BqTableSchemas()


# create empty Plaid accounts table.
def create_plaid_accounts_bq_table(access_tokens, plaid_country_codes):

    # get BQ schema information
    plaid_accounts_bq_schema = bq_tables.plaid_accounts()

    # create empty table to store account data
    bq.create_empty_bq_table(
        dataset_id=plaid_accounts_bq_schema["dataset_id"],
        table_id=plaid_accounts_bq_schema["table_prefix"],
        table_description=plaid_accounts_bq_schema["table_description"],
        table_schema=plaid_accounts_bq_schema["table_schema"],
    )

    # add access tokens to new empty accounts table
    for token in access_tokens:
        plaid_client.add_accounts_to_bq(
            access_token=token,
            dataset_id=plaid_accounts_bq_schema["dataset_id"],
            table_id=plaid_accounts_bq_schema["table_prefix"],
            plaid_country_codes=plaid_country_codes,
        )


def get_plaid_accounts():
    bq_schema = bq_tables.plaid_accounts()
    full_table_name = bq_schema["project_id"] + "." + bq_schema["dataset_id"] + "." + bq_schema["table_prefix"]

    query = f"""
    SELECT DISTINCT
      access_token,
      item_id
    FROM `{full_table_name}`
    """

    return bq.query(query)


# create empty Plaid accounts table.
def create_plaid_cursors_bq_table():

    # get BQ schema information
    bq_schema = bq_tables.plaid_cursors()
    partition_date = bq.get_partition_date(offset_days=0)
    table_id = bq_schema["table_prefix"] + "_" + partition_date

    # create empty table to store account data
    bq.create_empty_bq_table(
        dataset_id=bq_schema["dataset_id"],
        table_id=table_id,
        table_description=bq_schema["table_description"],
        table_schema=bq_schema["table_schema"],
    )

    # get plaid accounts. Stores access_token, item_id, and next cursor in df df
    accounts_df = get_plaid_accounts()

    # add empty cursor as next_cursor (fresh start)
    accounts_df["next_cursor"] = ""

    table_ref = bq_client.dataset(bq_schema["dataset_id"]).table(table_id)

    return bq_client.load_table_from_dataframe(accounts_df, table_ref)


# RUN THIS SHIT
create_plaid_accounts_bq_table(access_tokens=PLAID_ACCESS_TOKENS, plaid_country_codes=PLAID_COUNTRY_CODES)
create_plaid_cursors_bq_table()


# TODO: add crypto account here
# crypto code here

# get transactions
# account_cursors_table = bq.get_latest_table_partition(dataset_id="personal_finance", table_prefix="account_cursors_")

# cursors = plaid_client.get_latest_cursors()
# next_cursor = cursors[access_token]
# get_transactions(self, access_token, next_cursor):
