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
# initialize clients
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
plaid_accounts = PlaidAccounts(bq_client, plaid_client)
plaid_transactions = PlaidTransactions(bq_client, plaid_client)
bq_tables = BqTableSchemas()


# bq.create_empty_bq_table(
#     project_id="zsc-personal",
#     dataset_id="personal_finance",
#     table_id="test_table",
#     table_description="hello world",
#     table_schema=None,
#     write_disposition="WRITE_EMPTY",
# )

# bq.delete_bq_table(project_id="zsc-personal", dataset_id="personal_finance", table_id="test_table", confirm=False)

# bq.delete_bq_table(project_id="zsc-personal", dataset_id="personal_finance", table_id="test_table", confirm=False)

# plaid_cursors_bq = bq.update_bq_table_schema_new_partition(schema=bq_tables.plaid_cursors_YYYYMMDD(), offset_days=0)
# plaid_curs = bq_tables.plaid_cursors_YYYYMMDD()

# print(plaid_curs.full_table_name)

# Usage
cursors_df = plaid_transactions.get_latest_cursors()
print(cursors_df)
