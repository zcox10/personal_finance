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
# plaid_client.create_accounts_bq_table(
#     access_tokens=PLAID_ACCESS_TOKENS, plaid_country_codes=PLAID_COUNTRY_CODES, confirm=False
# )
# plaid_client.create_cursors_bq_table(confirm=False)

# create empty cursor table
plaid_client.create_temp_cursors_bq_table(confirm=False)

latest_cursors_df = plaid_client.get_latest_cursors()

print(latest_cursors_df["access_token"][0])
transactions_df, removed_df = plaid_client.get_transactions(
    access_token=latest_cursors_df["access_token"][0],
    item_id=latest_cursors_df["item_id"][0],
    next_cursor=latest_cursors_df["next_cursor"][0],
)

print("transactions_df")
print(transactions_df.dtypes)

print()

print("removed_df")
print(removed_df.dtypes)

print()
print("AUTH:", transactions_df["authorized_date"].max())
print("DATE:", transactions_df["date"].max())

print()
print("date >= date_removed")
print(transactions_df[transactions_df["date"] >= removed_df["date_removed"].max()].head())

print()
print("date < date_removed")
print(transactions_df[transactions_df["date"] < removed_df["date_removed"].max()].head())

# date_ = bq.get_date(offset_days=0)
# print(date_)
# print(type(date_))


# for i, row in latest_cursors_df.iterrows():
#     transactions_df, removed_transactions = plaid_client.get_transactions(
#         access_token=row["access_token"],
#         item_id=row["item_id"],
#         next_cursor=row["next_cursor"],
#     )
