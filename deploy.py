from google.cloud import bigquery
from utils.google_cloud_utils import GcpUtils
from schemas.cloud_schemas import CloudSchemas


def main():
    bq_client = bigquery.Client()
    gcp = GcpUtils(bq_client=bq_client)
    cs = CloudSchemas()

    # schemas to create jobs for
    schemas = [
        cs.personal_finance(),
        # cs.test_job(),
    ]

    for schema in schemas:
        # print("\n\nCREATE PUB/SUB:", schema.trigger_topic)
        # gcp.create_pubsub_topic(schema=schema, confirm=False)

        print("\n\nCREATE FUNCTION:", schema.function_name)
        gcp.create_cloud_function(schema=schema, show_output=True)

        # print("\n\nCREATE SCHEDULER:", schema.job_name)
        # gcp.create_scheduler_job(schema=schema, confirm=False)

        # # delete methods
        # gcp.delete_pubsub_topic(schema.project_id, schema.trigger_topic, confirm=False)
        # gcp.delete_scheduler_job(schema.project_id, schema.region, schema.job_name, confirm=False)
        # gcp.delete_cloud_function(
        #     schema.project_id, schema.region, schema.function_name, confirm=False
        # )


if __name__ == "__main__":
    main()
