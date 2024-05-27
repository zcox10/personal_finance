import base64
import time
import plaid
from google.cloud import bigquery
from datetime import datetime, timedelta

# utils
from utils.bq_utils import BqUtils
from utils.secrets_utils import SecretsUtils
from utils.plaid_utils import PlaidUtils
from utils.budget_values import BudgetValues
from utils.financial_accounts import FinancialAccounts
from utils.plaid_transactions import PlaidTransactions
from utils.plaid_investments import PlaidInvestments
from utils.query_jobs import QueryJobs

# SECRETS
secrets = SecretsUtils().create_secrets_dict(secret_type="PROD")
PLAID_CLIENT_ID = secrets["PLAID_CLIENT_ID"]
PLAID_SECRET = secrets["PLAID_SECRET_PROD"]
PLAID_ACCESS_TOKENS = SecretsUtils().get_access_token_secrets(secrets)
PLAID_HOST = plaid.Environment.Production

# CONSTANTS: general
WRITE_DISPOSITION = "WRITE_TRUNCATE"
OFFSET = 0

# CONSTANTS: plaid transactions
BACKFILL = False
ADD_TEST_TRANSACTIONS = False  # to add a removed transaction or not in generate_transactions_dfs()

# CONSTANTS: plaid investments
START_DATE = (  # if backfill, use 730 days ago as START_DATE. Else, use today
    (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    if BACKFILL
    else (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
)
END_DATE = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")

# initialize main clients
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)


def run_financial_accounts(event, context):
    # pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    # print(f"Received message: {pubsub_message}")

    print("STARTING financial_accounts")

    # init client
    financial_accounts = FinancialAccounts(bq_client, plaid_client)

    # Create a new financial_accounts table to, upload all account info according to access_tokens provided
    financial_accounts.create_empty_accounts_bq_table(OFFSET, WRITE_DISPOSITION)
    financial_accounts.add_plaid_accounts_to_bq(PLAID_ACCESS_TOKENS, ["US"], OFFSET)

    print("SUCCESS: Financial account data uploaded to BQ")


def run_budget_values(event, context):
    print("STARTING budget_values")

    # init client
    budget_values = BudgetValues(bq_client)

    # create most recent budget_values_df then upload to bq
    budget_values.upload_budget_values_df_to_bq(OFFSET)

    print("SUCCESS: Budget category data uploaded to BQ")


def run_plaid_transactions(event, context):
    print("STARTING plaid_transactions")

    # init client
    plaid_transactions = PlaidTransactions(bq_client, plaid_client)

    # check table dependencies
    table_list = [
        "zsc-personal.personal_finance.financial_accounts_YYYYMMDD",
        "zsc-personal.budget_values.budget_values_YYYYMM",
    ]
    bq.check_dependencies(table_list, OFFSET)

    # only create new financial_accounts table and plaid_cursors_YYYYMMDD table if starting with initial backfill
    if BACKFILL:
        # Create a new plaid_cursors_YYYYMMDD table with access_token, item_id, and next_cursor
        plaid_transactions.create_cursors_bq_table(PLAID_ACCESS_TOKENS, OFFSET, WRITE_DISPOSITION)

    # create empty temp cursor table to upload cursors to for the current run.
    # When job finishes running, this table will become the latest plaid_cursors_YYYYMMDD partitions
    plaid_transactions.create_temp_cursors_bq_table(write_disposition="WRITE_TRUNCATE")

    # grab latest cursors for each access token / item
    latest_cursors_df = plaid_transactions.get_latest_cursors(PLAID_ACCESS_TOKENS)

    # Run create_transactions_df() to store added/modified transactions in transactions_df and removed transactions in removed_df
    transactions_df_list, removed_df_list = plaid_transactions.generate_transactions_df_list(
        latest_cursors_df, OFFSET, ADD_TEST_TRANSACTIONS
    )

    # only upload transactions_df to BQ if there is at least one non-null df
    plaid_transactions.upload_transactions_df_list_to_bq(transactions_df_list, OFFSET, WRITE_DISPOSITION)

    # only upload removed_df to BQ if there is at least one non-null df
    plaid_transactions.upload_removed_df_list_to_bq(removed_df_list, OFFSET, WRITE_DISPOSITION)

    # Copy temp_cursors to plaid_cursors_YYYYMMDD
    plaid_transactions.copy_temp_cursors_to_cursors_bq_table(OFFSET, write_disposition="WRITE_TRUNCATE")

    print("SUCCESS: Plaid transactions data uploaded to BQ")


def run_plaid_investments(event, context):
    print("STARTING plaid_investments")

    # init client
    plaid_investments = PlaidInvestments(bq_client, plaid_client)

    # check table dependencies
    table_list = ["zsc-personal.personal_finance.financial_accounts_YYYYMMDD"]
    bq.check_dependencies(table_list, OFFSET)

    # get investments access_tokens
    access_tokens = list(
        plaid_client.get_items_by_access_token(PLAID_ACCESS_TOKENS, products=["investments"])["access_token"].unique()
    )

    # generate investment dfs for investment holdings and investment transactions
    holdings_df_list, investment_transactions_df_list = plaid_investments.generate_investments_dfs_list(
        START_DATE, END_DATE, access_tokens
    )

    # only upload holdings_df to BQ if there is at least one non-null df
    plaid_investments.upload_investment_holdings_df_list_to_bq(holdings_df_list, OFFSET, WRITE_DISPOSITION)

    # only upload investment_transactions_df to BQ if there is at least one non-null df
    plaid_investments.upload_investment_transactions_df_list_to_bq(
        investment_transactions_df_list, OFFSET, WRITE_DISPOSITION
    )

    print("SUCCESS: Plaid investment data uploaded to BQ")


def run_personal_finance_queries(event, context):
    query_jobs = QueryJobs(bq_client)
    query_jobs.create_tableau_table(WRITE_DISPOSITION, OFFSET)


def run_delete_tables(event, context):
    print("STARTING delete_tables")
    tables = [
        "financial_accounts_YYYYMMDD",
        "plaid_cursors_YYYYMMDD",
        "plaid_removed_transactions_YYYYMMDD",
        "plaid_transactions_YYYYMMDD",
        "temp_plaid_cursors",
        "plaid_investment_holdings_YYYYMMDD",
        "plaid_investment_transactions_YYYYMMDD",
    ]

    for table in tables:
        bq.delete_all_partitions(
            project_id="zsc-personal",
            dataset_id="personal_finance",
            table_id=table,
            confirm=False,
        )


# def main_test(event, context):
#     run_delete_tables("hello", "world")

#     time.sleep(3)

#     run_financial_accounts("hello", "world")

#     time.sleep(3)

#     run_budget_values("hello", "world")

#     time.sleep(3)

#     run_plaid_investments("hello", "world")

#     time.sleep(3)

#     run_plaid_transactions("hello", "world")

#     time.sleep(3)

#     run_personal_finance_queries("hello", "world")


# if __name__ == "__main__":
#     main_test("hello", "world")
