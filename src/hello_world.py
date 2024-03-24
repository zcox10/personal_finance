import pandas as pd
from google.cloud import bigquery

from utils.bq_utils import BqUtils
from utils.plaid_utils import PlaidUtils


print("Hello world")

bq_client_init = bigquery.Client()
bq = BqUtils(bq_client=bq_client_init)
print(bq.get_partition_date(offset_days=0))
