from google.cloud import bigquery
import plaid
from sql.bq_table_schemas import BqTableSchemas
from utils.plaid_utils import PlaidUtils
from utils.financial_accounts import FinancialAccounts

# constants
PLAID_CLIENT_ID = "65975384ab670e001c0aaf0d"
# PLAID_SECRET="56e33c77237c8c9e45f5c066b8b2fa" #production
PLAID_SECRET = "9294dd5ca4a5c99d90da56640f40e5"  # sandbox
PLAID_COUNTRY_CODES = ["US"]
PLAID_HOST = plaid.Environment.Sandbox
PLAID_ACCESS_TOKENS = [
    "access-sandbox-1902b6d6-33f3-49b8-a3ce-3213a5fe9a49",
    "access-sandbox-12481aea-33b6-4101-9f49-4e07ebbc1431",
    "access-sandbox-f415798a-74af-4e7f-b504-dc075dc5889c",
]
# PLAID_ENV = "sandbox"
# PLAID_PRODUCTS = ["liabilities", "transactions", "investments"]
# PLAID_REDIRECT_URI="https://localhost:3000/"

# initialize clients
bq_tables = BqTableSchemas()
bq_client = bigquery.Client()
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
financial_accounts = FinancialAccounts(bq_client, plaid_client)

### START HERE
write_disposition = "WRITE_TRUNCATE"
offset_days = -1

print("STARTING main_financial_accounts.py")

# Create a new financial_accounts table to, upload all account info according to access_tokens provided
financial_accounts.create_empty_accounts_bq_table(offset_days, write_disposition)
financial_accounts.add_plaid_accounts_to_bq(PLAID_ACCESS_TOKENS, PLAID_COUNTRY_CODES, offset_days)
