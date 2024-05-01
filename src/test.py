from google.cloud import bigquery
from utils.bq_utils import BqUtils

# from utils.secrets_utils import SecretsUtils


# sec = SecretsUtils()
# secrets = sec.create_secrets_dict(job_type="main_financial_accounts", secret_type="DEV")
# PLAID_CLIENT_ID = secrets["PLAID_CLIENT_ID"]
# PLAID_SECRET = secrets["PLAID_SECRET_DEV"]
# PLAID_ACCESS_TOKENS = sec.get_access_token_secrets(secrets)

bq_client = bigquery.Client()
bq = BqUtils(bq_client=bq_client)
offset_days = 0
