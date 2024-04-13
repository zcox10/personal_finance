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

plaid_cursors_bq = bq_tables.plaid_cursors_YYYYMMDD()


# RUN THIS SHIT
plaid_client.create_accounts_bq_table(
    access_tokens=PLAID_ACCESS_TOKENS, plaid_country_codes=PLAID_COUNTRY_CODES, confirm=False
)
plaid_client.create_cursors_bq_table(confirm=False)

# create empty cursor table
plaid_client.create_temp_cursors_bq_table(confirm=False)

# create empty transactions table
plaid_client.create_empty_transactions_bq_table(confirm=False)
plaid_client.create_empty_removed_bq_table(confirm=False)

# grab latest cursors for each access token / item
latest_cursors_df = plaid_client.get_latest_cursors()

# Run get_transactions() to store added/modified transactions in transactions_df and removed transactions in removed_df
# print(latest_cursors_df["access_token"][0])
transactions_df, removed_df = plaid_client.get_transactions(
    access_token=latest_cursors_df["access_token"][0],
    item_id=latest_cursors_df["item_id"][0],
    next_cursor=latest_cursors_df["next_cursor"][0],
)

# upload transactions and removed transactions to associated BQ tables
plaid_client.upload_transactions_df_to_bq(transactions_df)
plaid_client.upload_removed_df_to_bq(removed_df)
