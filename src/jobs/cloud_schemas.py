class CloudSchemas:
    def __init__(self):
        self.project_id = "zsc-personal"
        self.source = "."
        self.runtime = "python38"
        self.region = "us-west1"
        self.timeout = "540"
        self.memory = "512MB"
        self.service_account = "zsc-service-account@zsc-personal.iam.gserviceaccount.com"
        self.timezone = "UTC"

    def financial_accounts(self):
        return {
            "project_id": self.project_id,
            "function_name": "financial-accounts-workflow",
            "job_name": "financial-accounts-scheduler",
            "trigger_topic": "financial-accounts-pubsub",
            "entry_point": "run_financial_accounts",
            "source": self.source,
            "runtime": self.runtime,
            "region": self.region,
            "timeout": self.timeout,
            "memory": self.memory,
            "service_account": self.service_account,
            "timezone": self.timezone,
            "schedule": "0 10 * * *",  # 10:00am UTC
        }

    def budget_values(self):
        return {
            "project_id": self.project_id,
            "function_name": "budget-values-workflow",
            "job_name": "budget-values-scheduler",
            "trigger_topic": "budget-values-pubsub",
            "entry_point": "run_budget_values",
            "source": self.source,
            "runtime": self.runtime,
            "region": self.region,
            "timeout": self.timeout,
            "memory": self.memory,
            "service_account": self.service_account,
            "timezone": self.timezone,
            "schedule": "10 10 * * *",  # 10:10am UTC
        }

    def plaid_transactions(self):
        return {
            "project_id": self.project_id,
            "function_name": "plaid-transactions-workflow",
            "job_name": "plaid-transactions-scheduler",
            "trigger_topic": "plaid-transactions-pubsub",
            "entry_point": "run_plaid_transactions",
            "source": self.source,
            "runtime": self.runtime,
            "region": self.region,
            "timeout": self.timeout,
            "memory": self.memory,
            "service_account": self.service_account,
            "timezone": self.timezone,
            "schedule": "15 10 * * *",  # 10:15am UTC
        }

    def plaid_investments(self):
        return {
            "project_id": self.project_id,
            "function_name": "plaid-investments-workflow",
            "job_name": "plaid-investments-scheduler",
            "trigger_topic": "plaid-investments-pubsub",
            "entry_point": "run_plaid_investments",
            "source": self.source,
            "runtime": self.runtime,
            "region": self.region,
            "timeout": self.timeout,
            "memory": self.memory,
            "service_account": self.service_account,
            "timezone": self.timezone,
            "schedule": "30 10 * * *",  # 10:30am UTC
        }

    def personal_finance_queries(self):
        return {
            "project_id": self.project_id,
            "function_name": "personal-finance-queries-workflow",
            "job_name": "personal-finance-queries-scheduler",
            "trigger_topic": "personal-finance-queries-pubsub",
            "entry_point": "run_personal_finance_queries",
            "source": self.source,
            "runtime": self.runtime,
            "region": self.region,
            "timeout": self.timeout,
            "memory": self.memory,
            "service_account": self.service_account,
            "timezone": self.timezone,
            "schedule": "45 10 * * *",  # 10:45am UTC
        }

    def test_job(self):
        return {
            "project_id": self.project_id,
            "function_name": "test-function-workflow",
            "job_name": "test-scheduler",
            "trigger_topic": "test-pubsub",
            "entry_point": "main_test",
            "source": self.source,
            "runtime": self.runtime,
            "region": self.region,
            "timeout": self.timeout,
            "memory": self.memory,
            "service_account": self.service_account,
            "timezone": self.timezone,
            "schedule": "* * * * *",
        }
