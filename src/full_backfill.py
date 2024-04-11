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
plaid_client.create_accounts_bq_table(access_tokens=PLAID_ACCESS_TOKENS, plaid_country_codes=PLAID_COUNTRY_CODES)
plaid_client.create_cursors_bq_table()


# TODO: add crypto account here
# crypto code here

# get transactions
# account_cursors_table = bq.get_latest_table_partition(dataset_id="personal_finance", table_prefix="account_cursors_")

# cursors = plaid_client.get_latest_cursors()
# next_cursor = cursors[access_token]
# get_transactions(self, access_token, next_cursor):
