import time
import plaid
from google.cloud import bigquery
from datetime import datetime, timedelta

# utils
from utils.bq_utils import BqUtils
from utils.secrets_utils import SecretsUtils
from utils.plaid_utils import PlaidUtils
from utils.sendgrid_utils import SendgridUtils

# jobs
from jobs.budget_values import BudgetValues
from jobs.financial_accounts import FinancialAccounts
from jobs.plaid_transactions import PlaidTransactions
from jobs.plaid_investments import PlaidInvestments
from jobs.query_jobs import QueryJobs
from jobs.data_quality_alerts import DataQualityAlerts

# SECRETS
secrets = SecretsUtils().create_secrets_dict(plaid_secret_type="PROD")
PLAID_CLIENT_ID = secrets["PLAID_CLIENT_ID"]
PLAID_SECRET = secrets["PLAID_SECRET_PROD"]
PLAID_ACCESS_TOKENS = SecretsUtils().get_access_token_secrets(secrets)
PLAID_HOST = plaid.Environment.Production
CRYPTO_SECRETS = secrets["CRYPTO_SECRETS"]
SENDGRID_KEY = secrets["SENDGRID_KEY"]

# initialize main clients
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
plaid_client = PlaidUtils(bq_client, PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST)
dqa = DataQualityAlerts(bq_client, SENDGRID_KEY)

# CONSTANTS
WRITE_DISPOSITION = "WRITE_TRUNCATE"
OFFSET = 0  # for which partition to write to in BQ
BACKFILL = False  # to backfill plaid_investment_transactions and plaid_transactions
ADD_TEST_TRANSACTIONS = False  # to add a removed transaction or not in generate_transactions_dfs()


def run_financial_accounts(request):

    print("\n******************** STARTING financial_accounts ********************")

    # init client
    financial_accounts = FinancialAccounts(bq_client, plaid_client)

    # get crypto secrets
    eth_addresses = CRYPTO_SECRETS["ETH"]["addresses"]
    eth_api_key = CRYPTO_SECRETS["ETH"]["api_key"]
    btc_addresses = CRYPTO_SECRETS["BTC"]["addresses"]
    btc_api_key = CRYPTO_SECRETS["BTC"]["api_key"]

    # Create a new financial_accounts table to, upload all account info according to access_tokens provided
    financial_accounts.add_plaid_accounts_to_bq(
        PLAID_ACCESS_TOKENS,
        ["US"],
        eth_addresses,
        btc_addresses,
        eth_api_key,
        btc_api_key,
        OFFSET,
        WRITE_DISPOSITION,
    )
    print("SUCCESS: run_financial_accounts() complete!")
    return "hello-world"


def run_budget_values(request):
    print("\n******************** STARTING budget_values ********************")

    # init client
    budget_values = BudgetValues(bq_client)

    # create most recent budget_values_df then upload to bq
    budget_values.upload_budget_values_df_to_bq(OFFSET)

    print("SUCCESS: run_budget_values() complete!")
    return "hello-world"


def run_plaid_transactions(request):
    print("\n******************** STARTING plaid_transactions ********************")

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
    plaid_transactions.upload_transactions_df_list_to_bq(transactions_df_list, OFFSET)

    # only upload removed_df to BQ if there is at least one non-null df
    plaid_transactions.upload_removed_df_list_to_bq(removed_df_list, OFFSET, WRITE_DISPOSITION)

    # Copy temp_cursors to plaid_cursors_YYYYMMDD
    plaid_transactions.copy_temp_cursors_to_cursors_bq_table(
        OFFSET, write_disposition="WRITE_TRUNCATE"
    )

    print("SUCCESS: run_plaid_transactions() complete!")
    return "hello-world"


def run_plaid_investments(request):
    print("\n******************** STARTING plaid_investments ********************")

    # CONSTANTS: plaid investments
    START_DATE = (  # if backfill, use 730 days ago as START_DATE. Else, use (7 + OFFSET) days ago today
        (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        if BACKFILL
        else (datetime.now() - timedelta(days=(7 + OFFSET))).strftime("%Y-%m-%d")
    )
    END_DATE = (datetime.now() - timedelta(days=(6 + OFFSET))).strftime("%Y-%m-%d")

    # init client
    plaid_investments = PlaidInvestments(bq_client, plaid_client)

    # check table dependencies
    table_list = ["zsc-personal.personal_finance.financial_accounts_YYYYMMDD"]
    bq.check_dependencies(table_list, OFFSET)

    # get investments access_tokens
    access_tokens = list(
        plaid_client.get_items_by_access_token(PLAID_ACCESS_TOKENS, products=["investments"])[
            "access_token"
        ].unique()
    )

    # generate investment dfs for investment holdings and investment transactions
    holdings_df_list, investment_transactions_df_list = (
        plaid_investments.generate_investments_dfs_list(START_DATE, END_DATE, access_tokens)
    )

    # only upload holdings_df to BQ if there is at least one non-null df
    plaid_investments.upload_investment_holdings_df_list_to_bq(
        holdings_df_list, OFFSET, WRITE_DISPOSITION
    )

    # only upload investment_transactions_df to BQ if there is at least one non-null df
    plaid_investments.upload_investment_transactions_df_list_to_bq(
        investment_transactions_df_list, OFFSET, WRITE_DISPOSITION
    )
    print("SUCCESS: run_plaid_investments() complete!")
    return "hello-world"


def run_personal_finance_queries(request):
    print("\n******************** STARTING personal_finance_queries ********************")

    query_jobs = QueryJobs(bq_client)
    query_jobs.create_tableau_table(
        sql_path="queries/personal_finance_tableau.sql",
        offset=OFFSET,
        write_disposition=WRITE_DISPOSITION,
    )
    print("SUCCESS: run_personal_finance_queries() complete!")
    return "hello-world"


def run_dqa_checks(requeset):
    print("\n******************** STARTING DQA checks ********************")

    # current_date used in subject line
    current_date = datetime.today().strftime("%Y-%m-%d")
    from_email = "zach.s.cox@gmail.com"
    dqa.send_alert_messages(
        financial_accounts_sql_path="queries/dqa_financial_accounts.sql",
        investment_holdings_sql_path="queries/dqa_investment_holdings.sql",
        investment_transactions_sql_path="queries/dqa_investment_transactions.sql",
        budget_values_sql_path="queries/dqa_budget_values.sql",
        removed_transactions_sql_path="queries/dqa_removed_transactions.sql",
        plaid_transactions_sql_path="queries/dqa_plaid_transactions.sql",
        plaid_cursors_sql_path="queries/dqa_plaid_cursors.sql",
        tableau_sql_path="queries/dqa_tableau.sql",
        zero_threshold=0,
        pct_chg_threshold=10,
        from_email=from_email,
        to_emails=["zach.s.cox+zsc-alerts@gmail.com"],
        email_subject=f"{current_date} - ZSC Alerts",
    )

    dqa.send_status_message(
        sql_path="queries/dqa_daily_status.sql",
        from_email=from_email,
        to_emails=["zach.s.cox+zsc-status@gmail.com"],
        email_subject=f"{current_date} - ZSC Status",
    )
    print("SUCCESS: run_dqa_checks() complete!")
    return "hello-world"


def run_data_table_retention(request):
    print("\n******************** STARTING data_table_retention ********************")

    # only keep partitions in last 30d
    PROJECT_ID = "zsc-personal"
    DATASET_ID = "personal_finance"
    RETENTION_DATE = (datetime.now() - timedelta(days=(29))).strftime("%Y%m%d")

    tables = [
        # "plaid_cursors_YYYYMMDD",
        "personal_finance_tableau_YYYYMMDD",
    ]

    for table_id in tables:
        table_partitions = bq.get_table_range_partitions(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=table_id,
            start_date=None,
            end_date=RETENTION_DATE,
        )

    bq.delete_list_of_tables(
        project_id=PROJECT_ID, dataset_id=DATASET_ID, table_ids=table_partitions, confirm=False
    )
    print("SUCCESS: run_data_table_retention() complete!")
    return "hello-world"


def run_delete_latest_tables(request):
    print("\n******************** STARTING delete_latest_tables ********************")
    tables = [
        "financial_accounts_YYYYMMDD",
        "plaid_cursors_YYYYMMDD",
        "plaid_removed_transactions_YYYYMMDD",
        "plaid_transactions_YYYYMMDD",
        "temp_plaid_cursors",
        "plaid_investment_holdings_YYYYMMDD",
        "plaid_investment_transactions_YYYYMMDD",
        "personal_finance_tableau_YYYYMMDD",
    ]

    for table in tables:
        table = bq.get_latest_table_partition("personal_finance", table)
        bq.delete_bq_table(
            project_id="zsc-personal",
            dataset_id="personal_finance",
            table_id=table,
            confirm=False,
        )
    print("SUCCESS: run_delete_latest_tables() complete!")
    return "hello-world"


def main(request):
    # run_delete_latest_tables("hello-world")
    # time.sleep(3)

    # PROD
    run_financial_accounts("hello-world")
    time.sleep(3)
    run_budget_values("hello-world")
    time.sleep(3)
    run_plaid_investments("hello-world")
    time.sleep(3)
    run_plaid_transactions("hello-world")
    time.sleep(3)
    run_personal_finance_queries("hello-world")
    time.sleep(3)
    run_data_table_retention("hello-world")
    time.sleep(3)
    run_dqa_checks("hello-world")

    print("SUCCESS: all jobs complete!")
    return "hello-world"


# if __name__ == "__main__":
#     main("hello-world")
