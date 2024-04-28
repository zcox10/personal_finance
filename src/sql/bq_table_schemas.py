class BqTableSchemas:
    def __init__(self):
        pass

    def financial_accounts_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "financial_accounts_YYYYMMDD"
        full_table_name = project_id + "." + dataset_id + "." + table_id
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
                "name": "account_source",
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
                "name": "balance",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "available",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "current",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "limit",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "currency_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "unofficial_currency_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
            {
                "name": "access_token",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "update_type",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "consent_expiration_time",
                "type": "DATETIME",
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
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_cursors_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_cursors_YYYYMMDD"
        full_table_name = project_id + "." + dataset_id + "." + table_id
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
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def temp_plaid_cursors(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "temp_plaid_cursors"
        full_table_name = project_id + "." + dataset_id + "." + table_id
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
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_transactions_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_transactions_YYYYMMDD"
        full_table_name = project_id + "." + dataset_id + "." + table_id
        table_description = "Stores Plaid transactions, partitioned daily"
        table_schema = [
            {
                "name": "item_id",
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
                "name": "transaction_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "pending_transaction_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "is_pending",
                "type": "BOOL",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "account_owner",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "status",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "date",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "datetime",
                "type": "DATETIME",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "authorized_date",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "authorized_datetime",
                "type": "DATETIME",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "amount",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "currency_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "unofficial_currency_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "personal_finance_category",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "primary",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "detailed",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "confidence_level",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
            {
                "name": "payment_channel",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "merchant",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "entity_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "merchant_name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "website",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
            {
                "name": "counterparties",
                "type": "STRUCT",
                "mode": "REPEATED",
                "fields": [
                    {
                        "name": "entity_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "type",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "confidence_level",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "logo_url",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "website",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
            {
                "name": "location",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "address",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "city",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "region",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "postal_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "latitude",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "longitude",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
            {
                "name": "check_number",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "payment_meta",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "reference_number",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "ppd_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "by_order_of",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "payee",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "payer",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "payment_method",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "payment_processor",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "reason",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
            {
                "name": "transaction_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_removed_transactions_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_removed_transactions_YYYYMMDD"
        full_table_name = project_id + "." + dataset_id + "." + table_id
        table_description = "Stores Plaid removed transactions, partitioned daily"
        table_schema = [
            {
                "name": "item_id",
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
                "name": "transaction_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "date_removed",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "",
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_investment_holdings_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_investment_holdings_YYYYMMDD"
        full_table_name = project_id + "." + dataset_id + "." + table_id
        table_description = "Stores Plaid investment holdings, partitioned daily"
        table_schema = [
            {
                "name": "item_id",
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
                "name": "cost_basis",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "institution_price",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "institution_price_date",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "institution_price_datetime",
                "type": "DATETIME",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "institution_value",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "currency_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "unofficial_currency_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "quantity",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "vested_quantity",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "vested_value",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "security",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "security_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "currency_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "unofficial_currency_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "close_price",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "close_price_date",
                        "type": "DATE",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "update_datetime",
                        "type": "DATETIME",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "cusip",
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
                        "name": "institution_security_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "is_cash_equivalent",
                        "type": "BOOL",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "isin",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "market_identifier_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "option_contract",
                        "type": "STRUCT",
                        "mode": "NULLABLE",
                        "fields": [
                            {
                                "name": "contract_type",
                                "type": "STRING",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                            {
                                "name": "expiration_date",
                                "type": "STRING",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                            {
                                "name": "strike_price",
                                "type": "FLOAT",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                            {
                                "name": "underlying_security_ticker",
                                "type": "STRING",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                        ],
                    },
                    {
                        "name": "proxy_security_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "sedol",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "ticker_symbol",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "type",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema

    def plaid_investment_transactions_YYYYMMDD(self):
        project_id = "zsc-personal"
        dataset_id = "personal_finance"
        table_id = "plaid_investment_transactions_YYYYMMDD"
        full_table_name = project_id + "." + dataset_id + "." + table_id
        table_description = "Stores Plaid investment transactions, partitioned daily"
        table_schema = [
            {
                "name": "item_id",
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
                "name": "investment_transaction_id",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "date",
                "type": "DATE",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "name",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "quantity",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "amount",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "price",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "fees",
                "type": "FLOAT",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "type",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "subtype",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "currency_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "unofficial_currency_code",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "",
            },
            {
                "name": "security",
                "type": "STRUCT",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "security_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "currency_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "unofficial_currency_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "close_price",
                        "type": "FLOAT",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "close_price_date",
                        "type": "DATE",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "update_datetime",
                        "type": "DATETIME",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "cusip",
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
                        "name": "institution_security_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "is_cash_equivalent",
                        "type": "BOOL",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "isin",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "market_identifier_code",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "option_contract",
                        "type": "STRUCT",
                        "mode": "NULLABLE",
                        "fields": [
                            {
                                "name": "contract_type",
                                "type": "STRING",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                            {
                                "name": "expiration_date",
                                "type": "STRING",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                            {
                                "name": "strike_price",
                                "type": "FLOAT",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                            {
                                "name": "underlying_security_ticker",
                                "type": "STRING",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                        ],
                    },
                    {
                        "name": "proxy_security_id",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "sedol",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "ticker_symbol",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                    {
                        "name": "type",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "",
                    },
                ],
            },
        ]

        schema = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "full_table_name": full_table_name,
            "table_description": table_description,
            "table_schema": table_schema,
        }

        return schema
