import time
import pandas as pd
from google.cloud import bigquery
import plaid
from utils.plaid_utils import PlaidUtils
from utils.plaid_investments import PlaidInvestments

# constants
PLAID_CLIENT_ID = "65975384ab670e001c0aaf0d"
# PLAID_SECRET="56e33c77237c8c9e45f5c066b8b2fa" #production
PLAID_SECRET = "9294dd5ca4a5c99d90da56640f40e5"  # sandbox
PLAID_HOST = plaid.Environment.Sandbox
# PLAID_ENV = "sandbox"
# PLAID_PRODUCTS = ["liabilities", "transactions", "investments"]
# PLAID_COUNTRY_CODES = ["US"]
# PLAID_REDIRECT_URI="https://localhost:3000/"
# PLAID_ACCESS_TOKENS = [
#     "access-sandbox-1902b6d6-33f3-49b8-a3ce-3213a5fe9a49",
#     "access-sandbox-12481aea-33b6-4101-9f49-4e07ebbc1431",
#     "access-sandbox-f415798a-74af-4e7f-b504-dc075dc5889c",
# ]

# initialize clients
bq_client = bigquery.Client()
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
plaid_investments = PlaidInvestments(bq_client, plaid_client)

### START HERE
write_disposition = "WRITE_TRUNCATE"
offset_days = -1
start_date = "2024-01-01"
end_date = "2024-04-25"

print("STARTING main_plaid_investments.py")

# Run create_transactions_df() to store added/modified transactions in transactions_df and removed transactions in removed_df
holdings_df_list = []
investment_transactions_df_list = []

# get investments access_tokens
access_tokens = list(plaid_client.get_access_tokens(products=["investments"])["access_token"].unique())

# generate investment dfs for investment holdings and investment transactions
for token in access_tokens:
    holdings_df, investment_transactions_df = plaid_investments.generate_investments_dfs(start_date, end_date, token)
    holdings_df_list.append(holdings_df)
    investment_transactions_df_list.append(investment_transactions_df)

concat_holdings_df = pd.concat(holdings_df_list)
concat_investment_transactions_df = pd.concat(investment_transactions_df_list)

# only upload holdings_df to BQ if there is at least one non-null df
if not all(df is None for df in holdings_df_list):
    concat_holdings_df = pd.concat(holdings_df_list)

    # create empty plaid_investment_holdings_YYYYMMDD to upload holdings to
    plaid_investments.create_empty_investment_holdings_bq_table(
        offset_days=offset_days,
        write_disposition=write_disposition,
    )
    print("SLEEP 5 SECONDS TO WAIT FOR plaid_investment_holdings_YYYYMMDD creation\n")
    time.sleep(5)
    plaid_investments.upload_investment_holdings_df_to_bq(concat_holdings_df, offset_days)
else:
    print("No investment holdings present in concat_investment_holdings_df")

# only upload investment_transactions_df to BQ if there is at least one non-null df
if not all(df is None for df in investment_transactions_df_list):
    concat_investment_transactions_df = pd.concat(investment_transactions_df_list)

    # create empty plaid_investment_transactions_YYYYMMDD to upload transactions to
    plaid_investments.create_empty_investment_transactions_bq_table(
        offset_days=offset_days, write_disposition=write_disposition
    )
    print("SLEEP 5 SECONDS TO WAIT FOR plaid_investment_transactions_YYYYMMDD creation\n")
    time.sleep(5)
    plaid_investments.upload_investment_transactions_df_to_bq(concat_investment_transactions_df, offset_days)
else:
    print("No investment transactions present in concat_investment_transactions_df")

print("SUCCESS: investment data uploaded to BQ")
