import time
import pandas as pd
from google.cloud import bigquery
import plaid

from sql.bq_table_schemas import BqTableSchemas
from utils.bq_utils import BqUtils

# from utils.plaid_utils import PlaidUtils
from utils.plaid_utils import PlaidUtils
from utils.plaid_accounts import PlaidAccounts
from utils.plaid_transactions import PlaidTransactions
from utils.plaid_investments import PlaidInvestments


# constants
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

# initialize clients
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
plaid_accounts = PlaidAccounts(bq_client, plaid_client)
plaid_transactions = PlaidTransactions(bq_client, plaid_client)
plaid_investments = PlaidInvestments(bq_client, plaid_client)
bq_tables = BqTableSchemas()

### START HERE
backfill = True
write_disposition = "WRITE_TRUNCATE"
offset_days = 0

# only create new plaid_accounts table and plaid_cursors_YYYYMMDD table if starting with initial backfill
if backfill:
    print("STARTING HISTORICAL DATA PULL")
    # Create a new plaid_accounts table to, upload all account info according to access_tokens provided
    plaid_accounts.create_accounts_bq_table(
        access_tokens=PLAID_ACCESS_TOKENS, plaid_country_codes=PLAID_COUNTRY_CODES, write_disposition=write_disposition
    )

    # Create a new plaid_cursors_YYYYMMDD table with access_token, item_id, and next_cursor
    plaid_transactions.create_cursors_bq_table(offset_days=offset_days, write_disposition=write_disposition)

else:
    print("STARTING DAILY DATA PULL")

# create empty temp cursor table to upload cursors to for the current run.
# When job finishes running, this table will become the latest plaid_cursors_YYYYMMDD partitions
plaid_transactions.create_temp_cursors_bq_table(write_disposition="WRITE_TRUNCATE")

# create empty plaid_transactions_YYYYMMDD and plaid_removed_transactions_YYYYMMDD to upload transactions to
plaid_transactions.create_empty_transactions_bq_table(offset_days=offset_days, write_disposition=write_disposition)
plaid_transactions.create_empty_removed_bq_table(offset_days=offset_days, write_disposition=write_disposition)

# grab latest cursors for each access token / item
latest_cursors_df = plaid_transactions.get_latest_cursors()

# give time for tables to be created
print("SLEEP 10 SEC TO WAIT FOR BQ TABLES TO BE CREATED")
time.sleep(10)

# Run get_transactions() to store added/modified transactions in transactions_df and removed transactions in removed_df
for i, row in latest_cursors_df.iterrows():

    transactions_df, removed_df = plaid_transactions.get_transactions(
        access_token=row["access_token"],
        item_id=row["item_id"],
        next_cursor=row["next_cursor"],
        offset_days=offset_days,
    )

    # upload transactions and removed transactions to associated BQ tables
    if transactions_df is not None:
        plaid_transactions.upload_transactions_df_to_bq(transactions_df)
        print()
    else:
        print("No transactions present in transactions_df")

    if removed_df is not None:
        plaid_transactions.upload_removed_df_to_bq(removed_df)
        print()
    else:
        print("No removed transactions present in removed_df")

# Copy temp_cursors to plaid_cursors_YYYYMMDD
plaid_transactions.copy_temp_cursors_to_cursors_bq_table(offset_days=offset_days, write_disposition="WRITE_TRUNCATE")
