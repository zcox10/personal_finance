from google.cloud import bigquery
from utils.google_cloud_utils import GcpUtils
from schemas.cloud_schemas import CloudSchemas

bq_client = bigquery.Client()
gcp = GcpUtils(bq_client=bq_client)
cs = CloudSchemas()


# jobs to run
jobs = [
    cs.financial_accounts,
    cs.budget_values,
    cs.plaid_transactions,
    cs.plaid_investments,
    cs.personal_finance_queries,
    cs.data_table_retention,
]

for job in jobs:
    # print("CREATE PUB/SUB:", job["trigger_topic"])
    # gcp.create_pubsub_topic(
    #     project_id=job["project_id"],
    #     topic_name=job["trigger_topic"],
    #     labels=None,
    #     kms_key_name=None,
    #     message_retention_duration=None,
    #     confirm=False,
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

    # print("CREATE SCHEDULER:", job["trigger_topic"])
    # gcp.create_scheduler_job(
    #     project_id=job["project_id"],
    #     location=job["region"],
    #     job_name=job["job_name"],
    #     schedule=job["schedule"],
    #     timezone=job["timezone"],
    #     topic_name=job["trigger_topic"],
    #     payload=job["trigger_topic"],
    #     confirm=False,
    # )

    print()
