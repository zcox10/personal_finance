import plaid
import time
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from utils.plaid_utils import PlaidUtils
from utils.plaid_investments import PlaidInvestments

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
plaid_investments = PlaidInvestments(bq_client, plaid_client)

### START HERE
write_disposition = "WRITE_TRUNCATE"
offset_days = -1
start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
# start_date = "2024-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

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
