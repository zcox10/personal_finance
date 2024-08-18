import pandas as pd
from typing import List, Tuple, Any
from utils.bq_utils import BqUtils
from utils.sendgrid_utils import SendgridUtils


class DataQualityAlerts:

    def __init__(self, bq_client, sendgrid_api_key):
        self._bq = BqUtils(bq_client=bq_client)
        self._sendgrid = SendgridUtils(sendgrid_api_key=sendgrid_api_key)

    def get_single_bq_table_to_check(
        self, full_table_name: str, partition_type: str
    ) -> Tuple[str, str]:
        split_table_identifiers = full_table_name.split(".")
        dataset_id = split_table_identifiers[1]
        table_id = split_table_identifiers[2]
        table_prefix = table_id[: len(partition_type) * -1]

        full_table_name = self._bq.get_latest_full_table_name(dataset_id, table_id)
        return (
            full_table_name,
            self._bq.get_table_suffix(full_table_name.split(".")[2], table_prefix),
        )

    def get_latest_bq_tables_to_check(
        self, full_table_name: str, partition_type: str
    ) -> List[Tuple[str, str]]:
        """
        0d signifies the latest partition date
        1d signifies the penultimate (second-to-latest) partition date

        Returns latest and penultimate table names and partitions.
        """
        split_table_identifiers = full_table_name.split(".")
        dataset_id = split_table_identifiers[1]
        table_id = split_table_identifiers[2]
        table_prefix = table_id[: len(partition_type) * -1]

        full_table_name_0d = self._bq.get_latest_full_table_name(dataset_id, table_id)
        table_suffix_0d = self._bq.get_table_suffix(full_table_name_0d.split(".")[2], table_prefix)

        full_table_name_1d = self._bq.get_second_latest_full_table_name(dataset_id, table_id)
        table_suffix_1d = self._bq.get_table_suffix(full_table_name_1d.split(".")[2], table_prefix)

        return [
            (full_table_name_0d, table_suffix_0d),
            (full_table_name_1d, table_suffix_1d),
        ]

    def one_table_query(self, sql_path: str, full_table_name: str, partition_type: str) -> str:
        tables = self.get_single_bq_table_to_check(full_table_name, partition_type)
        query = self._bq.sql_file_to_string(sql_path)
        return query.format(
            full_table_name_0d=tables[0],
            table_suffix_0d=tables[1],
        )

    def two_table_query(self, sql_path: str, full_table_name: str, partition_type: str) -> str:
        tables = self.get_latest_bq_tables_to_check(full_table_name, partition_type)
        query = self._bq.sql_file_to_string(sql_path)
        return query.format(
            full_table_name_0d=tables[0][0],
            table_suffix_0d=tables[0][1],
            full_table_name_1d=tables[1][0],
            table_suffix_1d=tables[1][1],
        )

    def custom_tableau_query(self, sql_path: str, full_table_name: str, partition_type: str) -> str:
        tableau_tables = self.get_latest_bq_tables_to_check(full_table_name, partition_type)
        financial_account_tables = self.get_single_bq_table_to_check(
            full_table_name="zsc-personal.personal_finance.financial_accounts_YYYYMMDD",
            partition_type="YYYYMMDD",
        )
        query = self._bq.sql_file_to_string(sql_path)
        return query.format(
            full_table_name_0d=tableau_tables[0][0],
            table_suffix_0d=tableau_tables[0][1],
            full_table_name_1d=tableau_tables[1][0],
            table_suffix_1d=tableau_tables[1][1],
            financial_accounts_table_0d=financial_account_tables[0],
            financial_accounts_table_suffix_0d=financial_account_tables[1],
        )

    def create_null_alert(self, df: pd.DataFrame, zero_threshold: int, partition_field_name: str):
        """
        Gather all columns starting with null_ from provided df, then add up the values.
        If total value is > 0, there is a null value that should not be in the table partition
        """
        null_cols = [col for col in df.columns if col.startswith("null_")]
        if df[null_cols].sum().sum() != zero_threshold:
            return df[[partition_field_name] + null_cols]
        return None

    def create_null_alert_message(self, df: pd.DataFrame, table_name: str) -> str:
        return self._sendgrid.create_html_message_with_pandas_df(
            intro_text=f"Null item(s) in <b>{table_name}</b>",
            df=df,
        )

    def create_diff_alert(
        self,
        df: pd.DataFrame,
        partition_fields: List[str],
        zero_threshold: int,
        excluded_metric_names: List[str],
    ) -> Any:
        """
        Gather all columns starting with total_ and not {change_col} (via pct chg) from provided df.
        These will be used to count the number of entities in partition_0d vs. partition_1d.
        If there is a diff in the counts, throw an alert
        """
        count_cols = [
            col
            for col in df.columns
            if col.startswith("total_")
            and not any(excluded_metric in col for excluded_metric in excluded_metric_names)
        ]
        diff_cols = [col for col in count_cols if "diff" in col]

        if df[diff_cols].sum().sum() != zero_threshold:
            return df[partition_fields + count_cols]
        return None

    def create_diff_alert_message(self, df: pd.DataFrame, table_name: str) -> str:
        return self._sendgrid.create_html_message_with_pandas_df(
            intro_text=f"Diff entity counts in <b>{table_name}</b> for partition_0d vs. partition_1d",
            df=df,
        )

    def create_pct_chg_alert(self, df: pd.DataFrame, pct_chg_threshold: int, metric_name: str):
        """
        Gather all columns starting with total_ and "{metric_name}" for calculating pct chg in partition_0d vs. partition_1d.
        """
        chg_cols = [col for col in df.columns if col.startswith("total_") and metric_name in col]
        if df[f"total_{metric_name}_pct_chg_abs"][0] > pct_chg_threshold:
            return df[["partition_0d", "partition_1d"] + chg_cols]
        return None

    def create_pct_chg_alert_message(
        self, df: pd.DataFrame, table_name: str, pct_chg_threshold: int
    ) -> str:
        return self._sendgrid.create_html_message_with_pandas_df(
            intro_text=f"Entity value pct chg in <b>{table_name}</b> for partition_0d vs. partition_1d is > {pct_chg_threshold}%",
            df=df,
        )

    def create_empty_alert(self, df: pd.DataFrame, zero_threshold: int):
        """
        Check to ensure that table is not empty
        """
        if df["total_rows"][0] == zero_threshold:
            return df["partition_0d", "total_rows"]
        return None

    def create_empty_alert_message(self, df: pd.DataFrame, table_name: str) -> str:
        return self._sendgrid.create_html_message_with_pandas_df(
            intro_text=f"0 rows in <b>{table_name}</b>",
            df=df,
        )

    def create_missing_category_alert(self, df: pd.DataFrame, zero_threshold: int):
        """
        Check to ensure that table is not empty
        """
        if df["missing_categories_count"][0] > zero_threshold:
            return df[["partition_0d", "missing_categories", "missing_categories_count"]]
        return None

    def create_missing_category_alert_message(self, df: pd.DataFrame, table_name: str) -> str:
        return self._sendgrid.create_html_message_with_pandas_df(
            intro_text=f"Missing category in <b>budget_values_*</b>, but present in <b>{table_name}</b>",
            df=df,
        )

    def financial_accounts_full_check(
        self, sql_path: str, zero_threshold: int, pct_chg_threshold: int
    ) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "financial_accounts_YYYYMMDD"
        df = self._bq.query(
            self.two_table_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # diff check
        diff_df = self.create_diff_alert(
            df=df,
            partition_fields=["partition_0d", "partition_1d"],
            zero_threshold=zero_threshold,
            excluded_metric_names=["account_value"],
        )
        if diff_df is not None:
            final_messages.append(self.create_diff_alert_message(diff_df, table_name))

        # pct chg check
        pct_chg_df = self.create_pct_chg_alert(df, pct_chg_threshold, metric_name="account_value")
        if pct_chg_df is not None:
            final_messages.append(
                self.create_pct_chg_alert_message(pct_chg_df, table_name, pct_chg_threshold)
            )

        return final_messages

    def investment_holdings_full_check(
        self, sql_path: str, zero_threshold: int, pct_chg_threshold: int
    ) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "plaid_investment_holdings_YYYYMMDD"
        df = self._bq.query(
            self.two_table_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # diff check
        diff_df = self.create_diff_alert(
            df=df,
            partition_fields=["partition_0d", "partition_1d"],
            zero_threshold=zero_threshold,
            excluded_metric_names=["investment_value"],
        )
        if diff_df is not None:
            final_messages.append(self.create_diff_alert_message(diff_df, table_name))

        # pct chg check
        pct_chg_df = self.create_pct_chg_alert(
            df, pct_chg_threshold, metric_name="investment_value"
        )
        if pct_chg_df is not None:
            final_messages.append(
                self.create_pct_chg_alert_message(pct_chg_df, table_name, pct_chg_threshold)
            )

        return final_messages

    def investment_transactions_full_check(self, sql_path: str, zero_threshold: int) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "plaid_investment_transactions_YYYYMMDD"
        df = self._bq.query(
            self.one_table_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # empty table check (0 rows)
        empty_df = self.create_empty_alert(df, zero_threshold)
        if empty_df is not None:
            final_messages.append(self.create_empty_alert_message(empty_df, table_name))

        return final_messages

    def budget_values_full_check(
        self, sql_path: str, zero_threshold: int, pct_chg_threshold: int
    ) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "budget_values_YYYYMM"
        df = self._bq.query(
            self.two_table_query(
                sql_path,
                full_table_name=f"zsc-personal.budget_values.{table_name}",
                partition_type="YYYYMM",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partitions_null")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # diff check
        diff_df = self.create_diff_alert(
            df=df,
            partition_fields=["partition_0d", "partition_1d"],
            zero_threshold=zero_threshold,
            excluded_metric_names=["budget_amount"],
        )
        if diff_df is not None:
            final_messages.append(self.create_diff_alert_message(diff_df, table_name))

        # commenting out because budget changes cause this alert to be quite sensitive
        # # pct chg check
        # pct_chg_df = self.create_pct_chg_alert(df, pct_chg_threshold, metric_name="budget_amount")
        # if pct_chg_df is not None:
        #     final_messages.append(
        #         self.create_pct_chg_alert_message(pct_chg_df, table_name, pct_chg_threshold)
        #     )

        return final_messages

    def plaid_cursors_full_check(self, sql_path: str, zero_threshold: int) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "plaid_cursors_YYYYMMDD"
        df = self._bq.query(
            self.one_table_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # empty table check (0 rows)
        empty_df = self.create_empty_alert(df, zero_threshold)
        if empty_df is not None:
            final_messages.append(self.create_empty_alert_message(empty_df, table_name))

        return final_messages

    def removed_transactions_full_check(self, sql_path: str, zero_threshold: int) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "plaid_removed_transactions_YYYYMMDD"
        df = self._bq.query(
            self.one_table_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # empty table check (0 rows)
        empty_df = self.create_empty_alert(df, zero_threshold)
        if empty_df is not None:
            final_messages.append(self.create_empty_alert_message(empty_df, table_name))

        return final_messages

    def plaid_transactions_full_check(self, sql_path: str, zero_threshold: int) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "plaid_transactions_YYYYMMDD"
        df = self._bq.query(
            self.one_table_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # empty table check (0 rows)
        category_df = self.create_missing_category_alert(df, zero_threshold)
        if category_df is not None:
            final_messages.append(
                self.create_missing_category_alert_message(category_df, table_name)
            )

        return final_messages

    def tableau_full_check(
        self, sql_path: str, zero_threshold: int, pct_chg_threshold: int
    ) -> List[str]:
        final_messages = []

        # generate main df
        table_name = "personal_finance_tableau_YYYYMMDD"
        df = self._bq.query(
            self.custom_tableau_query(
                sql_path,
                full_table_name=f"zsc-personal.personal_finance.{table_name}",
                partition_type="YYYYMMDD",
            )
        )

        # null check
        null_df = self.create_null_alert(df, zero_threshold, partition_field_name="partition_0d")
        if null_df is not None:
            final_messages.append(self.create_null_alert_message(null_df, table_name))

        # diff check
        diff_df = self.create_diff_alert(
            df=df,
            partition_fields=["partition_0d", "partition_1d", "partition_fin_accts"],
            zero_threshold=zero_threshold,
            excluded_metric_names=["account_summed_value", "actual_amount"],
        )
        if diff_df is not None:
            final_messages.append(self.create_diff_alert_message(diff_df, table_name))

        # pct chg check
        account_summed_value_pct_chg_df = self.create_pct_chg_alert(
            df, pct_chg_threshold, metric_name="account_summed_value"
        )
        if account_summed_value_pct_chg_df is not None:
            final_messages.append(
                self.create_pct_chg_alert_message(
                    account_summed_value_pct_chg_df, table_name, pct_chg_threshold
                )
            )

        actual_amount_pct_chg_df = self.create_pct_chg_alert(
            df, pct_chg_threshold, metric_name="actual_amount"
        )
        if actual_amount_pct_chg_df is not None:
            final_messages.append(
                self.create_pct_chg_alert_message(
                    actual_amount_pct_chg_df, table_name, pct_chg_threshold
                )
            )

        return final_messages

    def send_status_message(
        self,
        sql_path: str,
        from_email: str,
        to_emails: List[str],
        email_subject: str,
    ) -> List[str]:

        df = self._bq.query(self._bq.sql_file_to_string(sql_path))

        html_message = self._sendgrid.create_html_message_with_pandas_df(
            intro_text=f"Status check for <b>personal_finance</b> tables",
            df=df,
        )

        email_message = self._sendgrid.construct_email_message(
            from_email=from_email,
            to_emails=to_emails,
            email_subject=email_subject,
            html_message=html_message,
        )
        self._sendgrid.send_email(email_message)

    # FINAL MESSAGES
    def aggregate_alerts(
        self,
        financial_accounts_sql_path,
        investment_holdings_sql_path,
        investment_transactions_sql_path,
        budget_values_sql_path,
        removed_transactions_sql_path,
        plaid_transactions_sql_path,
        plaid_cursors_sql_path,
        tableau_sql_path,
        zero_threshold,
        pct_chg_threshold,
    ):
        # all messages
        financial_accounts_messages = self.financial_accounts_full_check(
            financial_accounts_sql_path, zero_threshold, pct_chg_threshold
        )
        investment_holdings_messages = self.investment_holdings_full_check(
            investment_holdings_sql_path, zero_threshold, pct_chg_threshold
        )
        investment_transactions_messages = self.investment_transactions_full_check(
            investment_transactions_sql_path, zero_threshold
        )
        budget_values_messages = self.budget_values_full_check(
            budget_values_sql_path, zero_threshold, pct_chg_threshold
        )
        removed_transactions_messages = self.removed_transactions_full_check(
            removed_transactions_sql_path, zero_threshold
        )
        plaid_transactions_messages = self.plaid_transactions_full_check(
            plaid_transactions_sql_path, zero_threshold
        )
        plaid_cursors_messages = self.plaid_cursors_full_check(
            plaid_cursors_sql_path, zero_threshold
        )
        tableau_messages = self.tableau_full_check(
            tableau_sql_path, zero_threshold, pct_chg_threshold
        )

        # list of html strings for each alert
        return (
            financial_accounts_messages
            + investment_holdings_messages
            + investment_transactions_messages
            + budget_values_messages
            + removed_transactions_messages
            + plaid_transactions_messages
            + plaid_cursors_messages
            + tableau_messages
        )

    def send_alert_messages(
        self,
        financial_accounts_sql_path,
        investment_holdings_sql_path,
        investment_transactions_sql_path,
        budget_values_sql_path,
        removed_transactions_sql_path,
        plaid_transactions_sql_path,
        plaid_cursors_sql_path,
        tableau_sql_path,
        zero_threshold,
        pct_chg_threshold,
        from_email,
        to_emails,
        email_subject,
    ):
        messages = self.aggregate_alerts(
            financial_accounts_sql_path,
            investment_holdings_sql_path,
            investment_transactions_sql_path,
            budget_values_sql_path,
            removed_transactions_sql_path,
            plaid_transactions_sql_path,
            plaid_cursors_sql_path,
            tableau_sql_path,
            zero_threshold,
            pct_chg_threshold,
        )

        # if there are any alerts to send, send them. Else :)
        if len(messages) > 0:
            html_message = self._sendgrid.chain_html_messages(messages)
            email_message = self._sendgrid.construct_email_message(
                from_email=from_email,
                to_emails=to_emails,
                email_subject=email_subject,
                html_message=html_message,
            )
            self._sendgrid.send_email(email_message)
        else:
            print("SUCCESS: No alert messages to send :)")
