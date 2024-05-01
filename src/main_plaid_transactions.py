import plaid
from google.cloud import bigquery
from utils.bq_utils import BqUtils
from utils.secrets_utils import SecretsUtils
from utils.plaid_utils import PlaidUtils
from utils.plaid_transactions import PlaidTransactions

# CONSTANTS
print("STARTING main_plaid_transactions.py")
WRITE_DISPOSITION = "WRITE_EMPTY"
OFFSET_DAYS = 0
BACKFILL = True
ADD_TEST_TRANSACTIONS = False  # to add a removed transaction or not in generate_transactions_dfs()

# check table dependencies
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
table_list = ["zsc-personal.personal_finance.financial_accounts_YYYYMMDD"]
bq.check_dependencies(table_list, OFFSET_DAYS)

# get all secrets
sec = SecretsUtils()
secrets = sec.create_secrets_dict(job_type="main_plaid_transactions", secret_type="DEV")
plaid_client_id = secrets["PLAID_CLIENT_ID"]
plaid_secret = secrets["PLAID_SECRET_DEV"]
plaid_host = plaid.Environment.Development

# initialize clients
bq_client = bigquery.Client()
plaid_client = PlaidUtils(bq_client, plaid_client_id, plaid_secret, plaid_host)
plaid_transactions = PlaidTransactions(bq_client, plaid_client)

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
