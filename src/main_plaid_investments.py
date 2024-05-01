import plaid
import time
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from utils.secrets_utils import SecretsUtils
from utils.bq_utils import BqUtils
from utils.plaid_utils import PlaidUtils
from utils.plaid_investments import PlaidInvestments

# CONSTANTS
print("STARTING main_plaid_investments.py")
WRITE_DISPOSITION = "WRITE_EMPTY"
OFFSET_DAYS = 0
START_DATE = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
# start_date = datetime.now().strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

# check table dependencies
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
table_list = ["zsc-personal.personal_finance.financial_accounts_YYYYMMDD"]
bq.check_dependencies(table_list, offset_days=OFFSET_DAYS)

# get all secrets
sec = SecretsUtils()
secrets = sec.create_secrets_dict(job_type="main_plaid_investments", secret_type="DEV")
plaid_client_id = secrets["PLAID_CLIENT_ID"]
plaid_secret = secrets["PLAID_SECRET_DEV"]
plaid_host = plaid.Environment.Development

# initialize final clients
plaid_client = PlaidUtils(bq_client, plaid_client_id, plaid_secret, plaid_host)
plaid_investments = PlaidInvestments(bq_client, plaid_client)

# get investments access_tokens
access_tokens = list(plaid_client.get_access_tokens(products=["investments"])["access_token"].unique())

# generate investment dfs for investment holdings and investment transactions
holdings_df_list, investment_transactions_df_list = plaid_investments.generate_investments_dfs_list(
    START_DATE, END_DATE, access_tokens
)

# only upload holdings_df to BQ if there is at least one non-null df
plaid_investments.upload_investment_holdings_df_list_to_bq(holdings_df_list, OFFSET_DAYS, WRITE_DISPOSITION)

# only upload investment_transactions_df to BQ if there is at least one non-null df
plaid_investments.upload_investment_transactions_df_list_to_bq(
    investment_transactions_df_list, OFFSET_DAYS, WRITE_DISPOSITION
)

print("SUCCESS: Plaid investment data uploaded to BQ")
