from google.cloud import bigquery
import plaid
from sql.bq_table_schemas import BqTableSchemas
from utils.plaid_utils import PlaidUtils
from utils.financial_accounts import FinancialAccounts

# constants
PLAID_CLIENT_ID = "65975384ab670e001c0aaf0d"
# PLAID_SECRET = "56e33c77237c8c9e45f5c066b8b2fa"  # production
# PLAID_SECRET = "9294dd5ca4a5c99d90da56640f40e5"  # sandbox
PLAID_SECRET = "c5c55de38434db3e6456d0e146db8b"  # dev
PLAID_HOST = plaid.Environment.Development
PLAID_ACCESS_TOKENS = [
    "access-development-a00c51da-ea66-459a-9d70-aa8e7cde48db",  # Chase
    "access-development-36b11b1f-7e28-41b7-bff1-713683d6d180",  # BoA
    "access-development-71716c32-af8e-4632-8805-dc26872a0187",  # Schwab
    "access-development-07939f94-059d-45d0-a338-65222b5ea656",  # Vanguard
    "access-development-d73b7fc4-f2ee-4a52-9d80-a06f203a2009",  # Fundrise
    "access-development-b34d5d0a-eca4-4fa8-8b27-e8bf8ef37dc6",  # e-Trade
]
PLAID_COUNTRY_CODES = ["US"]
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
