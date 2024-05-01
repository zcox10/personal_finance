from google.cloud import bigquery
import plaid
from sql.bq_table_schemas import BqTableSchemas
from utils.secrets_utils import SecretsUtils
from utils.plaid_utils import PlaidUtils
from utils.financial_accounts import FinancialAccounts

# CONSTANTS
print("STARTING main_financial_accounts.py")
write_disposition = "WRITE_EMPTY"
offset_days = 0

# get all secrets
sec = SecretsUtils()
secrets = sec.create_secrets_dict(job_type="main_financial_accounts", secret_type="DEV")
PLAID_CLIENT_ID = secrets["PLAID_CLIENT_ID"]
PLAID_SECRET = secrets["PLAID_SECRET_DEV"]
PLAID_ACCESS_TOKENS = sec.get_access_token_secrets(secrets)
PLAID_HOST = plaid.Environment.Development

# initialize clients
bq_tables = BqTableSchemas()
bq_client = bigquery.Client()
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
financial_accounts = FinancialAccounts(bq_client, plaid_client)

# Create a new financial_accounts table to, upload all account info according to access_tokens provided
financial_accounts.create_empty_accounts_bq_table(offset_days, write_disposition)
financial_accounts.add_plaid_accounts_to_bq(PLAID_ACCESS_TOKENS, ["US"], offset_days)

print("SUCCESS: Financial account data uploaded to BQ")
