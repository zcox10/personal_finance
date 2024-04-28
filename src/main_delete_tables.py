from google.cloud import bigquery
from utils.bq_utils import BqUtils

# initialize clients
bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)

### START HERE
tables = [
    "plaid_accounts",
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
