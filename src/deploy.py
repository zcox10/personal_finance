from google.cloud import bigquery
from utils.google_cloud_utils import GcpUtils
from schemas.cloud_schemas import CloudSchemas


def main():
    bq_client = bigquery.Client()
    gcp = GcpUtils(bq_client=bq_client)
    cs = CloudSchemas()

    # schemas to create jobs for
    schemas = [
        cs.financial_accounts(),
        cs.budget_values(),
        cs.plaid_transactions(),
        cs.plaid_investments(),
        cs.personal_finance_queries(),
        cs.data_table_retention(),
    ]

    for schema in schemas:
        print("\n\nCREATE PUB/SUB:", schema.trigger_topic)
        gcp.create_pubsub_topic(schema=schema, confirm=False)

        print("\n\nCREATE FUNCTION:", schema.function_name)
        gcp.create_cloud_function(schema=schema, show_output=True)

        print("\n\nCREATE SCHEDULER:", schema.job_name)
        gcp.create_scheduler_job(schema=schema, confirm=False)


if __name__ == "__main__":
    main()
