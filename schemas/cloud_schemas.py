class CloudSchema:
    def __init__(self, function_name, job_name, trigger_topic, entry_point, schedule):
        self.project_id = "zsc-personal"

        # trigger topic type
        self.target_type = "http"  # options are "http" or "pubsub"

        # http
        self.http_method = "GET"  # e.g. "POST" or "GET"

        # pubsub
        self.trigger_topic = trigger_topic  # pubsub topic name, also picked up via cloud functions
        self.labels = None  # pubsub optional arg
        self.kms_key_name = None  # pubsub optional arg
        self.message_retention_duration = None  # pubsub optional arg

        # cloud scheduler
        self.timezone = "UTC"
        self.job_name = job_name  # designated cloud scheduler name
        self.payload = trigger_topic  # cannot be null, so filling with trigger_topic
        self.schedule = schedule  # cron

        # cloud functions
        self.service_account = "zsc-service-account@zsc-personal.iam.gserviceaccount.com"
        self.source = "."  # source dir
        self.runtime = "python39"  # language version to run
        self.timeout = "540"  # in seconds, limit is 10 min
        self.memory = "512MB"  # memory allocated to the function
        self.entry_point = entry_point  # starting function to run
        self.function_name = function_name  # designated cloud function name

        # multiple jobs
        self.region = "us-west1"  # cloud scheduler and cloud functions


class CloudSchemas:

    def financial_accounts(self):
        return CloudSchema(
            entry_point="run_financial_accounts",
            function_name="financial-accounts-workflow",
            trigger_topic="financial-accounts-pubsub",
            job_name="financial-accounts-scheduler",
            schedule="0 10 * * *",  # 10:00am UTC
        )

    def budget_values(self):
        return CloudSchema(
            entry_point="run_budget_values",
            function_name="budget-values-workflow",
            trigger_topic="budget-values-pubsub",
            job_name="budget-values-scheduler",
            schedule="10 10 * * *",  # 10:10am UTC
        )

    def plaid_transactions(self):
        return CloudSchema(
            entry_point="run_plaid_transactions",
            function_name="plaid-transactions-workflow",
            trigger_topic="plaid-transactions-pubsub",
            job_name="plaid-transactions-scheduler",
            schedule="15 10 * * *",  # 10:15am UTC
        )

    def plaid_investments(self):
        return CloudSchema(
            entry_point="run_plaid_investments",
            function_name="plaid-investments-workflow",
            trigger_topic="plaid-investments-pubsub",
            job_name="plaid-investments-scheduler",
            schedule="30 10 * * *",  # 10:30am UTC
        )

    def personal_finance_queries(self):
        return CloudSchema(
            entry_point="run_personal_finance_queries",
            function_name="personal-finance-queries-workflow",
            trigger_topic="personal-finance-queries-pubsub",
            job_name="personal-finance-queries-scheduler",
            schedule="45 10 * * *",  # 10:45am UTC
        )

    def data_table_retention(self) -> dict:
        return CloudSchema(
            entry_point="run_data_table_retention",
            function_name="data-table-retention-workflow",
            trigger_topic="data-table-retention-pubsub",
            job_name="data-table-retention-scheduler",
            schedule="0 11 * * *",  # 11:00am UTC
        )

    def test_job(self) -> dict:
        return CloudSchema(
            entry_point="main_test",
            function_name="test-function-workflow",
            trigger_topic="test-pubsub",
            job_name="test-scheduler",
            schedule="* * * * *",
        )
