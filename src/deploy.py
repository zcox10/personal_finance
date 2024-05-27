from google.cloud import bigquery
from utils.gcp_utils import GcpUtils
from jobs.cloud_schemas import CloudSchemas

bq_client = bigquery.Client()
gcp = GcpUtils(bq_client=bq_client)
cs = CloudSchemas()


financial_accounts = cs.financial_accounts()
budget_values = cs.budget_values()
plaid_transactions = cs.plaid_transactions()
plaid_investments = cs.plaid_investments()
personal_finance_queries = cs.personal_finance_queries()

# jobs to run
jobs = [
    financial_accounts,
    budget_values,
    plaid_transactions,
    plaid_investments,
    personal_finance_queries,
]

for job in jobs:
    # print("CREATE PUB/SUB:", job["trigger_topic"])
    # gcp.create_pubsub_topic(
    #     project_id=job["project_id"],
    #     topic_name=job["trigger_topic"],
    #     labels=None,
    #     kms_key_name=None,
    #     message_retention_duration=None,
    # )

    # print("CREATE SCHEDULER:", job["trigger_topic"])
    # gcp.create_scheduler_job(
    #     project_id=job["project_id"],
    #     location=job["region"],
    #     job_name=job["job_name"],
    #     schedule=job["schedule"],
    #     timezone=job["timezone"],
    #     topic_name=job["trigger_topic"],
    #     payload=job["trigger_topic"],
    #     confirm=True,
    # )

    print("CREATE FUNCTION:", job["function_name"])
    gcp.create_cloud_function(
        function_name=job["function_name"],
        trigger_topic=job["trigger_topic"],
        source=job["source"],
        runtime=job["runtime"],
        entry_point=job["entry_point"],
        region=job["region"],
        timeout=job["timeout"],
        memory=job["memory"],
        service_account=job["service_account"],
        show_output=True,
    )

    print()
