import time
import pandas as pd
from google.cloud import bigquery
import plaid
from utils.plaid_utils import PlaidUtils
from utils.plaid_transactions import PlaidTransactions

# constants
PLAID_CLIENT_ID = "65975384ab670e001c0aaf0d"
# PLAID_SECRET = "56e33c77237c8c9e45f5c066b8b2fa"  # production
# PLAID_SECRET = "9294dd5ca4a5c99d90da56640f40e5"  # sandbox
PLAID_SECRET = "c5c55de38434db3e6456d0e146db8b"  # dev
PLAID_HOST = plaid.Environment.Development
# PLAID_ACCESS_TOKENS = [
#     "access-development-a00c51da-ea66-459a-9d70-aa8e7cde48db",  # Chase
#     "access-development-36b11b1f-7e28-41b7-bff1-713683d6d180",  # BoA
#     "access-development-71716c32-af8e-4632-8805-dc26872a0187",  # Schwab
#     "access-development-07939f94-059d-45d0-a338-65222b5ea656",  # Vanguard
#     "access-development-d73b7fc4-f2ee-4a52-9d80-a06f203a2009",  # Fundrise
#     "access-development-b34d5d0a-eca4-4fa8-8b27-e8bf8ef37dc6",  # e-Trade
# ]
# PLAID_ENV = "sandbox"
# PLAID_PRODUCTS = ["liabilities", "transactions", "investments"]
# PLAID_COUNTRY_CODES = ["US"]
# PLAID_REDIRECT_URI="https://localhost:3000/"

# initialize clients
bq_client = bigquery.Client()
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
plaid_transactions = PlaidTransactions(bq_client, plaid_client)

### START HERE
backfill = True
write_disposition = "WRITE_TRUNCATE"
offset_days = -1

# only create new financial_accounts table and plaid_cursors_YYYYMMDD table if starting with initial backfill
if backfill:
    add_test_transaction = True  # to add a removed transaction or not in generate_transactions_dfs()
    print("STARTING HISTORICAL DATA PULL")
    # Create a new plaid_cursors_YYYYMMDD table with access_token, item_id, and next_cursor
    plaid_transactions.create_cursors_bq_table(offset_days=offset_days, write_disposition=write_disposition)

else:
    add_test_transaction = False
    print("STARTING DAILY DATA PULL")

# create empty temp cursor table to upload cursors to for the current run.
# When job finishes running, this table will become the latest plaid_cursors_YYYYMMDD partitions
plaid_transactions.create_temp_cursors_bq_table(write_disposition="WRITE_TRUNCATE")

# grab latest cursors for each access token / item
latest_cursors_df = plaid_transactions.get_latest_cursors()

# Run create_transactions_df() to store added/modified transactions in transactions_df and removed transactions in removed_df
transactions_df_list = []
removed_df_list = []
for i, row in latest_cursors_df.iterrows():

    transactions_df, removed_df = plaid_transactions.generate_transactions_dfs(
        access_token=row["access_token"],
        item_id=row["item_id"],
        next_cursor=row["next_cursor"],
        offset_days=offset_days,
        add_test_transaction=add_test_transaction,
    )

    if transactions_df is not None:
        transactions_df_list.append(transactions_df)

    if removed_df is not None:
        removed_df_list.append(removed_df)

# only upload transactions_df to BQ if there is at least one non-null df
if len(transactions_df_list) > 0:
    concat_transactions_df = pd.concat(transactions_df_list)
    plaid_transactions.create_empty_transactions_bq_table(offset_days=offset_days, write_disposition=write_disposition)
    print("SLEEP 5 SECONDS TO WAIT FOR plaid_transactions_YYYYMMDD creation\n")
    time.sleep(5)

    plaid_transactions.upload_transactions_df_to_bq(concat_transactions_df, offset_days)
else:
    print("No transactions present in concat_transactions_df")

# only upload removed_df to BQ if there is at least one non-null df
if len(removed_df_list) > 0:
    concat_removed_df = pd.concat(removed_df_list)
    plaid_transactions.create_empty_removed_bq_table(offset_days=offset_days, write_disposition=write_disposition)
    print("SLEEP 5 SECONDS TO WAIT FOR plaid_removed_transactions_YYYYMMDD creation\n")
    time.sleep(5)
    plaid_transactions.upload_removed_df_to_bq(concat_removed_df, offset_days)
else:
    print("No removed transactions present in concat_removed_df")

# Copy temp_cursors to plaid_cursors_YYYYMMDD
plaid_transactions.copy_temp_cursors_to_cursors_bq_table(offset_days=offset_days, write_disposition="WRITE_TRUNCATE")
