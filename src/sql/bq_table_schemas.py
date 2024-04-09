from google.cloud import bigquery


class BqTableSchemas:
    def __init__(self):
        pass

    def plaid_accounts(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_prefix = "plaid_accounts"
        table_description = "Stores all Plaid account data.  Field descriptions available at https://plaid.com/docs/api/accounts/#accounts-get-response-accounts-persistent-account-id"
        table_schema = [
            bigquery.SchemaField(name="item_id", field_type="STRING", mode="NULLABLE", description=""),
            bigquery.SchemaField(
                name="persistent_account_id",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(name="account_id", field_type="STRING", mode="NULLABLE", description=""),
            bigquery.SchemaField(
                name="account_mask",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_name",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_official_name",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_type",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="account_subtype",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="institution_id",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="institution_name",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                name="access_token",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(name="products", field_type="STRING", mode="REPEATED", description=""),
            bigquery.SchemaField(
                name="billed_products",
                field_type="STRING",
                mode="REPEATED",
                description="",
            ),
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_prefix": table_prefix,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_cursors(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_prefix = "plaid_cursors"
        table_description = "Stores Plaid item/access token cursors as of the latest run. Use latest partition to get most recently provided cursor"
        table_schema = [
            bigquery.SchemaField(name="item_id", field_type="STRING", mode="NULLABLE", description=""),
            bigquery.SchemaField(
                name="access_token",
                field_type="STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(name="next_cursor", field_type="STRING", mode="NULLABLE", description=""),
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_prefix": table_prefix,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema
