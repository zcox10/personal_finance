class BqTableSchemas:
    def __init__(self):
        pass

    def plaid_accounts(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_accounts"
        table_description = "Stores all Plaid account data.  Field descriptions available at https://plaid.com/docs/api/accounts/#accounts-get-response-accounts-persistent-account-id"
        table_schema = [
            {
                "name": "item_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "persistent_account_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_mask",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_official_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_type",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_subtype",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "institution_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "institution_name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "access_token",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "products",
                "type": "STRING",
                "mode": "REPEATED",
                "description": "",
            },
            {
                "name": "billed_products",
                "type": "STRING",
                "mode": "REPEATED",
                "description": "",
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_cursors_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_cursors_YYYYMMDD"
        table_description = "Stores Plaid item/access token cursors as of the latest run. Use latest partition to get most recently provided cursor"
        table_schema = [
            {
                "name": "item_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "access_token",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "next_cursor",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def temp_plaid_cursors(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "temp_plaid_cursors"
        table_description = "Stores temporary Plaid item/access token cursors as of the latest run. Updates continuously until workflow finishes running, then adds all final accounts/cursors to new plaid_cursors_YYYYMMDD partition"
        table_schema = [
            {
                "name": "item_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "access_token",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "next_cursor",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema
