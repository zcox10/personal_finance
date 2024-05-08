import base64
import time
import plaid
from google.cloud import bigquery
from datetime import datetime, timedelta

# utils
from utils.bq_utils import BqUtils
from utils.secrets_utils import SecretsUtils
from utils.plaid_utils import PlaidUtils
from utils.financial_accounts import FinancialAccounts
from utils.plaid_transactions import PlaidTransactions
from utils.plaid_investments import PlaidInvestments

# SECRETS
secrets = SecretsUtils().create_secrets_dict(job_type="main_financial_accounts", secret_type="DEV")
PLAID_CLIENT_ID = secrets["PLAID_CLIENT_ID"]
PLAID_SECRET = secrets["PLAID_SECRET_DEV"]
PLAID_ACCESS_TOKENS = SecretsUtils().get_access_token_secrets(secrets)
PLAID_HOST = plaid.Environment.Development

# CONSTANTS: general
WRITE_DISPOSITION = "WRITE_TRUNCATE"
OFFSET_DAYS = 0

# CONSTANTS: plaid transactions
BACKFILL = False
ADD_TEST_TRANSACTIONS = False  # to add a removed transaction or not in generate_transactions_dfs()

# CONSTANTS: plaid investments
# START_DATE = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
START_DATE = datetime.now().strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

# initialize main clients
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)


def run_financial_accounts(event, context):
    # pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    # print(f"Received message: {pubsub_message}")

    print("STARTING main_financial_accounts.py")

    # init client
    financial_accounts = FinancialAccounts(bq_client, plaid_client)

    # Create a new financial_accounts table to, upload all account info according to access_tokens provided
    financial_accounts.create_empty_accounts_bq_table(OFFSET_DAYS, WRITE_DISPOSITION)
    financial_accounts.add_plaid_accounts_to_bq(PLAID_ACCESS_TOKENS, ["US"], OFFSET_DAYS)

    print("SUCCESS: Financial account data uploaded to BQ")


def run_plaid_transactions(event, context):
    print("STARTING main_plaid_transactions.py")

    # init client
    plaid_transactions = PlaidTransactions(bq_client, plaid_client)

    # check table dependencies
    table_list = ["zsc-personal.personal_finance.financial_accounts_YYYYMMDD"]
    bq.check_dependencies(table_list, OFFSET_DAYS)

    # only create new financial_accounts table and plaid_cursors_YYYYMMDD table if starting with initial backfill
    if BACKFILL:
        # Create a new plaid_cursors_YYYYMMDD table with access_token, item_id, and next_cursor
        plaid_transactions.create_cursors_bq_table(OFFSET_DAYS, WRITE_DISPOSITION)

    # create empty temp cursor table to upload cursors to for the current run.
    # When job finishes running, this table will become the latest plaid_cursors_YYYYMMDD partitions
    plaid_transactions.create_temp_cursors_bq_table(write_disposition="WRITE_TRUNCATE")

    # grab latest cursors for each access token / item
    latest_cursors_df = plaid_transactions.get_latest_cursors()

    # Run create_transactions_df() to store added/modified transactions in transactions_df and removed transactions in removed_df
    transactions_df_list, removed_df_list = plaid_transactions.generate_transactions_df_list(
        latest_cursors_df, OFFSET_DAYS, ADD_TEST_TRANSACTIONS
    )

    # only upload transactions_df to BQ if there is at least one non-null df
    plaid_transactions.upload_transactions_df_list_to_bq(transactions_df_list, OFFSET_DAYS, WRITE_DISPOSITION)

    # only upload removed_df to BQ if there is at least one non-null df
    plaid_transactions.upload_removed_df_list_to_bq(removed_df_list, OFFSET_DAYS, WRITE_DISPOSITION)

    # Copy temp_cursors to plaid_cursors_YYYYMMDD
    plaid_transactions.copy_temp_cursors_to_cursors_bq_table(OFFSET_DAYS, write_disposition="WRITE_TRUNCATE")

    print("SUCCESS: Plaid transactions data uploaded to BQ")


def run_plaid_investments(event, context):
    print("STARTING main_plaid_investments.py")

    # init client
    plaid_investments = PlaidInvestments(bq_client, plaid_client)

    # check table dependencies
    table_list = ["zsc-personal.personal_finance.financial_accounts_YYYYMMDD"]
    bq.check_dependencies(table_list, offset_days=OFFSET_DAYS)

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


def run_delete_tables(event, context):
    print("STARTING main_delete_tables.py")
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


def main_test(event, context):
    run_delete_tables("hello", "world")

    time.sleep(10)

    run_financial_accounts("hello", "world")

    time.sleep(10)

    run_plaid_investments("hello", "world")

    time.sleep(10)

    run_plaid_transactions("hello", "world")


if __name__ == "__main__":
    main_test("hello", "world")
