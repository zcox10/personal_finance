class CloudSchema:
    def __init__(self, function_name, job_name, trigger_topic, entry_point, schedule):
        self.project_id = "zsc-personal"
        self.source = "."
        self.runtime = "python39"
        self.region = "us-west1"
        self.timeout = "540"
        self.memory = "512MB"
        self.service_account = "zsc-service-account@zsc-personal.iam.gserviceaccount.com"
        self.timezone = "UTC"
        self.function_name = function_name
        self.job_name = job_name
        self.trigger_topic = trigger_topic
        self.entry_point = entry_point
        self.schedule = schedule
        self.labels = None  # pubsub optional arg
        self.kms_key_name = None  # pubsub optional arg
        self.message_retention_duration = None  # pubsub optional arg
        self.payload = trigger_topic


class CloudSchemas:

    def financial_accounts(self):
        return CloudSchema(
            function_name="financial-accounts-workflow",
            job_name="financial-accounts-scheduler",
            trigger_topic="financial-accounts-pubsub",
            entry_point="run_financial_accounts",
            schedule="0 10 * * *",  # 10:00am UTC
        )

    def budget_values(self):
        return CloudSchema(
            function_name="budget-values-workflow",
            job_name="budget-values-scheduler",
            trigger_topic="budget-values-pubsub",
            entry_point="run_budget_values",
            schedule="10 10 * * *",  # 10:10am UTC
        )

    def plaid_transactions(self):
        return CloudSchema(
            function_name="plaid-transactions-workflow",
            job_name="plaid-transactions-scheduler",
            trigger_topic="plaid-transactions-pubsub",
            entry_point="run_plaid_transactions",
            schedule="15 10 * * *",  # 10:15am UTC
        )

    def plaid_investments(self):
        return CloudSchema(
            function_name="plaid-investments-workflow",
            job_name="plaid-investments-scheduler",
            trigger_topic="plaid-investments-pubsub",
            entry_point="run_plaid_investments",
            schedule="30 10 * * *",  # 10:30am UTC
        )

    def personal_finance_queries(self):
        return CloudSchema(
            function_name="personal-finance-queries-workflow",
            job_name="personal-finance-queries-scheduler",
            trigger_topic="personal-finance-queries-pubsub",
            entry_point="run_personal_finance_queries",
            schedule="45 10 * * *",  # 10:45am UTC
        )

    def data_table_retention(self) -> dict:
        return CloudSchema(
            function_name="data-table-retention-workflow",
            job_name="data-table-retention-scheduler",
            trigger_topic="data-table-retention-pubsub",
            entry_point="run_data_table_retention",
            schedule="00 11 * * *",  # 11:00am UTC
        )

    def test_job(self) -> dict:
        return CloudSchema(
            function_name="test-function-workflow",
            job_name="test-scheduler",
            trigger_topic="test-pubsub",
            entry_point="main_test",
            schedule="* * * * *",
        )
