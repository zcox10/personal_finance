import pandas as pd
from utils.bq_utils import BqUtils
from schemas.bq_table_schemas import BqTableSchemas


class BudgetValues:
    def __init__(self, bq_client):
        self._bq = BqUtils(bq_client=bq_client)
        self._bq_tables = BqTableSchemas()

    def _backfill_value_exists(self, entry, partition_month):
        """
        Checks if a backfill value exists for a given partition month in the provided entry.

        Args:
            entry (dict): The entry dictionary to check for backfill values.
            partition_month (str): The partition month to match with backfill dates, in 'YYYY-MM-DD' format.

        Returns:
            bool: True if the 'backfill' key exists and is a dictionary containing the partition_month,
                False otherwise.
        """
        # "backfill" key exists within the category entry
        if not ("backfill" in entry):
            return False

        # "backfill" value is a dict
        if not isinstance(entry["backfill"], dict):
            return False

        # the partition_month is present in the "backfill" dict
        if not (partition_month in entry["backfill"]):
            return False

        return True

    def _update_budget_schema_with_backfill_values(self, budget_schema, partition_month):
        """
        Updates the budget_schema with backfill values if partition_month matches any keys in backfill.

        Args:
            budget_schema (list of dict): The budget schema data.
            partition_month (str): The partition month to match with backfill dates in 'YYYY-MM-DD' format.

        Returns:
            list of dict: Updated budget schema with the budget_amount replaced by backfill values if applicable.
        """
        updated_schema = []

        for entry in budget_schema:
            # Create a copy of the entry to modify
            updated_entry = entry.copy()

            # Check if backfill key is present
            if self._backfill_value_exists(entry, partition_month):
                updated_entry["budget_amount"] = updated_entry["backfill"][partition_month]

            # Append the updated entry to the new schema
            updated_schema.append(updated_entry)

        return updated_schema

    def _create_budget_values_df(self, partition_month):
        """
        Generates budget_values_df via self._budget_schema() and self._update_budget_schema_with_backfill_values()
        """

        budget_schema = self._budget_schema()
        updated_schema = self._update_budget_schema_with_backfill_values(
            budget_schema, partition_month
        )

        return pd.DataFrame(
            updated_schema,
            columns=[
                "category_raw",
                "subcategory_raw",
                "category",
                "subcategory",
                "detail_category",
                "budget_amount",
            ],
        )

    def upload_budget_values_df_to_bq(self, offset):
        """
        Upload the budget_values_df to a new budget_values_YYYYMM BQ table

        Args:
            budget_values_df (pandas.DataFrame): the dataframe containing all budget categories and amounts
            offset (int): The offset to be applied to a given partition month

        Returns:
            google.cloud.bigquery.job.LoadJob: A BigQuery load job object representing the process of loading
            data into the created BigQuery table.
        """

        # get BQ schema information
        budget_values_bq = self._bq.update_table_schema_partition(
            self._bq_tables.budget_values_YYYYMM(),
            offset=offset,
        )

        partition_month = (
            self._bq.get_date(offset, partition_format="YYYYMM").replace(day=1).strftime("%Y-%m-%d")
        )

        # get budget_values_df
        budget_values_df = self._create_budget_values_df(partition_month)

        # upload df to budget_values_YYYYMM. "WRITE_TRUNCATE" because multiple transaction_df's will be loaded
        return self._bq.load_df_to_bq(
            budget_values_df,
            budget_values_bq.full_table_name,
            budget_values_bq.table_schema,
            "WRITE_TRUNCATE",
        )

    def _budget_schema(self):
        """
        The budget schema including categories and budget amounts
        """

        return [
            # INCOME
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_DIVIDENDS",
                "category": "Income",
                "subcategory": "Dividends",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_INTEREST_EARNED",
                "category": "Income",
                "subcategory": "Interest Earned",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_RETIREMENT_PENSION",
                "category": "Income",
                "subcategory": "Retirement Pension",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_TAX_REFUND",
                "category": "Income",
                "subcategory": "Tax Refund",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_UNEMPLOYMENT",
                "category": "Income",
                "subcategory": "Unemployment",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_WAGES",
                "category": "Income",
                "subcategory": "Wages",
                "detail_category": None,
                "budget_amount": 0.0,
                # "budget_amount": 7000.0,
            },
            {
                "category_raw": "INCOME",
                "subcategory_raw": "INCOME_OTHER_INCOME",
                "category": "Income",
                "subcategory": "Other",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            # TRANSFER_IN, positive amounts
            {
                "category_raw": "TRANSFER_IN",
                "subcategory_raw": "TRANSFER_IN_CASH_ADVANCES_AND_LOANS",
                "category": "Income",
                "subcategory": "Deposit",
                "detail_category": "Cash Advances and Loans",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_IN",  # important: deposit to BoA Checking or Savings, either cash or check deposit. Positive amounts
                "subcategory_raw": "TRANSFER_IN_DEPOSIT",
                "category": "Income",
                "subcategory": "Deposit",
                "detail_category": "Traditional",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_IN",  # deposit from Schwab/investment acct to BoA, positive amounts
                "subcategory_raw": "TRANSFER_IN_INVESTMENT_AND_RETIREMENT_FUNDS",
                "category": "Income",
                "subcategory": "Deposit",
                "detail_category": "Investment Funds",
                "budget_amount": 0.0,  # COLUMBIA MONEY WILL GO HERE
            },
            {
                "category_raw": "TRANSFER_IN",  # not used
                "subcategory_raw": "TRANSFER_IN_SAVINGS",
                "category": "Income",
                "subcategory": "Deposit",
                "detail_category": "Savings",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_IN",  # deposits from Zelle, Venmo, etc. to BoA
                "subcategory_raw": "TRANSFER_IN_ACCOUNT_TRANSFER",
                "category": "Income",
                "subcategory": "Deposit",
                "detail_category": "Account Transfer",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_IN",
                "subcategory_raw": "TRANSFER_IN_OTHER_TRANSFER_IN",
                "category": "Income",
                "subcategory": "Deposit",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # TRANSFER_OUT, negative amounts
            {
                "category_raw": "TRANSFER_OUT",  # Coinbase
                "subcategory_raw": "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_CRYPTO",
                "category": "Personal Investments",
                "subcategory": "Crypto",
                "detail_category": None,
                # "budget_amount": -666.66,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_OUT",  # Fundrise
                "subcategory_raw": "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_REAL_ESTATE",
                "category": "Personal Investments",
                "subcategory": "Real Estate",
                "detail_category": None,
                # "budget_amount": -1666.66,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_OUT",  # Schwab
                "subcategory_raw": "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_STOCKS",
                "category": "Personal Investments",
                "subcategory": "Stocks",
                "detail_category": None,
                # "budget_amount": -1700.0,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_OUT",  # transfer from Savings to BoA checking
                "subcategory_raw": "TRANSFER_OUT_SAVINGS",
                "category": "Transfer",
                "subcategory": "Out",
                "detail_category": "Savings",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_OUT",  # withdrawal cash, negative amounts
                "subcategory_raw": "TRANSFER_OUT_WITHDRAWAL",
                "category": "Personal Spending",
                "subcategory": "Cash Withdrawal",
                "detail_category": None,
                "budget_amount": -100.0,
            },
            {
                "category_raw": "TRANSFER_OUT",  # CLUSTER FUCK - lots of transfers: BetterBrand (bagels), Palisades, from Checking -> Savings, and BoA -> Venmo/Zelle
                "subcategory_raw": "TRANSFER_OUT_ACCOUNT_TRANSFER",
                "category": "Personal Spending",
                "subcategory": "Personal Payments",
                "detail_category": "Account Transfer",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSFER_OUT",  # random purchases: 118 wallet, ETS ERG (GRE), Cold Beers & Cheeseburgers
                "subcategory_raw": "TRANSFER_OUT_OTHER_TRANSFER_OUT",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # LOAN_PAYMENTS
            {
                "category_raw": "LOAN_PAYMENTS",
                "subcategory_raw": "LOAN_PAYMENTS_CAR_PAYMENT",
                "category": "Expenses",
                "subcategory": "Loan Payments",
                "detail_category": "Car Payment",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "LOAN_PAYMENTS",  # credit card payments, these should all be excluded
                "subcategory_raw": "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT",
                "category": "Expenses",
                "subcategory": "Loan Payments",
                "detail_category": "Credit Card",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "LOAN_PAYMENTS",  # one payment from Bill Pay (Pay Ready Parent), perhaps rent?
                "subcategory_raw": "LOAN_PAYMENTS_PERSONAL_LOAN_PAYMENT",
                "category": "Expenses",
                "subcategory": "Loan Payments",
                "detail_category": "Personal Loan",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "LOAN_PAYMENTS",
                "subcategory_raw": "LOAN_PAYMENTS_MORTGAGE_PAYMENT",
                "category": "Expenses",
                "subcategory": "Loan Payments",
                "detail_category": "Mortgage",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "LOAN_PAYMENTS",
                "subcategory_raw": "LOAN_PAYMENTS_STUDENT_LOAN_PAYMENT",
                "category": "Expenses",
                "subcategory": "Loan Payments",
                "detail_category": "Student Loan",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "LOAN_PAYMENTS",
                "subcategory_raw": "LOAN_PAYMENTS_OTHER_PAYMENT",
                "category": "Expenses",
                "subcategory": "Loan Payments",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # EXCLUDE_CATEGORY
            {  # to exclude transactions that should not be included e.g. transfers between accts and credit card payments
                "category_raw": "EXCLUDE_CATEGORY",
                "subcategory_raw": "EXCLUDE_CATEGORY",
                "category": "Exclude",
                "subcategory": "Exclude",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            # BANK_FEES
            {
                "category_raw": "BANK_FEES",
                "subcategory_raw": "BANK_FEES_ATM_FEES",
                "category": "Personal Spending",
                "subcategory": "Bank Fees",
                "detail_category": "ATM Fees",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "BANK_FEES",
                "subcategory_raw": "BANK_FEES_FOREIGN_TRANSACTION_FEES",
                "category": "Personal Spending",
                "subcategory": "Bank Fees",
                "detail_category": "Foreign Transaction Fees",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "BANK_FEES",
                "subcategory_raw": "BANK_FEES_INSUFFICIENT_FUNDS",
                "category": "Personal Spending",
                "subcategory": "Bank Fees",
                "detail_category": "Insufficient Funds",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "BANK_FEES",
                "subcategory_raw": "BANK_FEES_INTEREST_CHARGE",
                "category": "Personal Spending",
                "subcategory": "Bank Fees",
                "detail_category": "Interest Charge",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "BANK_FEES",
                "subcategory_raw": "BANK_FEES_OVERDRAFT_FEES",
                "category": "Personal Spending",
                "subcategory": "Bank Fees",
                "detail_category": "Overdraft Fees",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "BANK_FEES",
                "subcategory_raw": "BANK_FEES_OTHER_BANK_FEES",
                "category": "Personal Spending",
                "subcategory": "Bank Fees",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # ENTERTAINMENT
            {
                "category_raw": "ENTERTAINMENT",
                "subcategory_raw": "ENTERTAINMENT_CASINOS_AND_GAMBLING",
                "category": "Personal Spending",
                "subcategory": "Entertainment",
                "detail_category": "Gambling",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "ENTERTAINMENT",  # CREDIT from Spotify should be income, other charges from Spotify (DEBIT) are legit
                "subcategory_raw": "ENTERTAINMENT_MUSIC_AND_AUDIO",
                "category": "Personal Spending",
                "subcategory": "Entertainment",
                "detail_category": "Music and Audio",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "ENTERTAINMENT",  # mostly golf
                "subcategory_raw": "ENTERTAINMENT_SPORTING_EVENTS_AMUSEMENT_PARKS_AND_MUSEUMS",
                "category": "Personal Spending",
                "subcategory": "Entertainment",
                "detail_category": "General",
                "budget_amount": -100.0,
            },
            {
                "category_raw": "ENTERTAINMENT",  # mostly Amazon Prime Video rentals
                "subcategory_raw": "ENTERTAINMENT_TV_AND_MOVIES",
                "category": "Personal Spending",
                "subcategory": "Entertainment",
                "detail_category": "TV and Movies",
                "budget_amount": -20.0,
            },
            {
                "category_raw": "ENTERTAINMENT",  # valid, video games
                "subcategory_raw": "ENTERTAINMENT_VIDEO_GAMES",
                "category": "Personal Spending",
                "subcategory": "Entertainment",
                "detail_category": "Video Games",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "ENTERTAINMENT",
                "subcategory_raw": "ENTERTAINMENT_OTHER_ENTERTAINMENT",
                "category": "Personal Spending",
                "subcategory": "Entertainment",
                "detail_category": "General",
                "budget_amount": 0.0,
            },
            # FOOD_AND_DRINK
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_BEER_WINE_AND_LIQUOR",
                "category": "Personal Spending",
                "subcategory": "Food and Drink",
                "detail_category": "Alcohol",
                # "budget_amount": -100.0,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_COFFEE",
                "category": "Personal Spending",
                "subcategory": "Food and Drink",
                "detail_category": "Coffee",
                # "budget_amount": -20.0,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_FAST_FOOD",
                "category": "Personal Spending",
                "subcategory": "Food and Drink",
                "detail_category": "Fast Food",
                "budget_amount": -50.0,
            },
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_GROCERIES",
                "category": "Expenses",
                "subcategory": "Groceries",
                "detail_category": None,
                "budget_amount": -400.0,
            },
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_RESTAURANT",
                "category": "Personal Spending",
                "subcategory": "Food and Drink",
                "detail_category": "Restaurant",
                "budget_amount": -100.0,
            },
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_VENDING_MACHINES",
                "category": "Personal Spending",
                "subcategory": "Food and Drink",
                "detail_category": "Vending Machines",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "FOOD_AND_DRINK",
                "subcategory_raw": "FOOD_AND_DRINK_OTHER_FOOD_AND_DRINK",
                "category": "Personal Spending",
                "subcategory": "Food and Drink",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # GENERAL_MERCHANDISE
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_BOOKSTORES_AND_NEWSSTANDS",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Media",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_CLOTHING_AND_ACCESSORIES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Clothing",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_CONVENIENCE_STORES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Convenience Stores",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_DEPARTMENT_STORES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Department Stores",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_DISCOUNT_STORES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Discount Stores",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_ELECTRONICS",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Electronics",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_GIFTS_AND_NOVELTIES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Gifts",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_OFFICE_SUPPLIES",
                "category": "Expenses",
                "subcategory": "Education",
                "detail_category": "Office Supplies",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_ONLINE_MARKETPLACES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Online",
                "budget_amount": -100.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_PET_SUPPLIES",
                "category": "Expenses",
                "subcategory": "Pets",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_SPORTING_GOODS",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Sporting Goods",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_SUPERSTORES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Superstores",
                "budget_amount": -200.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_TOBACCO_AND_VAPE",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Tobacco and Vape",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_MERCHANDISE",
                "subcategory_raw": "GENERAL_MERCHANDISE_OTHER_GENERAL_MERCHANDISE",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # HOME_IMPROVEMENT
            {
                "category_raw": "HOME_IMPROVEMENT",
                "subcategory_raw": "HOME_IMPROVEMENT_FURNITURE",
                "category": "Personal Spending",
                "subcategory": "Home Improvement",
                "detail_category": "Furniture",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "HOME_IMPROVEMENT",
                "subcategory_raw": "HOME_IMPROVEMENT_HARDWARE",
                "category": "Personal Spending",
                "subcategory": "Home Improvement",
                "detail_category": "Hardware",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "HOME_IMPROVEMENT",
                "subcategory_raw": "HOME_IMPROVEMENT_REPAIR_AND_MAINTENANCE",
                "category": "Personal Spending",
                "subcategory": "Home Improvement",
                "detail_category": "Repair and Maintenance",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "HOME_IMPROVEMENT",
                "subcategory_raw": "HOME_IMPROVEMENT_SECURITY",
                "category": "Personal Spending",
                "subcategory": "Home Improvement",
                "detail_category": "Security",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "HOME_IMPROVEMENT",
                "subcategory_raw": "HOME_IMPROVEMENT_OTHER_HOME_IMPROVEMENT",
                "category": "Personal Spending",
                "subcategory": "Home Improvement",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # MEDICAL
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_DENTAL_CARE",
                "category": "Expenses",
                "subcategory": "Medical",
                "detail_category": "Dental",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_EYE_CARE",
                "category": "Expenses",
                "subcategory": "Medical",
                "detail_category": "Eye Care",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_NURSING_CARE",
                "category": "Expenses",
                "subcategory": "Medical",
                "detail_category": "Nursing Care",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_PHARMACIES_AND_SUPPLEMENTS",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "General Stores",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_PRIMARY_CARE",
                "category": "Expenses",
                "subcategory": "Medical",
                "detail_category": "Primary Care",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_VETERINARY_SERVICES",
                "category": "Expenses",
                "subcategory": "Medical",
                "detail_category": "Veterinary Services",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "MEDICAL",
                "subcategory_raw": "MEDICAL_OTHER_MEDICAL",
                "category": "Expenses",
                "subcategory": "Medical",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # PERSONAL_CARE
            {
                "category_raw": "PERSONAL_CARE",
                "subcategory_raw": "PERSONAL_CARE_GYMS_AND_FITNESS_CENTERS",
                "category": "Expenses",
                "subcategory": "Fitness",
                "detail_category": "Gyms",
                "budget_amount": -10.0,
            },
            {
                "category_raw": "PERSONAL_CARE",
                "subcategory_raw": "PERSONAL_CARE_HAIR_AND_BEAUTY",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Hair and Beauty",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "PERSONAL_CARE",
                "subcategory_raw": "PERSONAL_CARE_LAUNDRY_AND_DRY_CLEANING",
                "category": "Expenses",
                "subcategory": "Laundry",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "PERSONAL_CARE",
                "subcategory_raw": "PERSONAL_CARE_OTHER_PERSONAL_CARE",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # GENERAL_SERVICES
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_ACCOUNTING_AND_FINANCIAL_PLANNING",
                "category": "Expenses",
                "subcategory": "Financial Planning",
                "detail_category": "General",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_AUTOMOTIVE",
                "category": "Expenses",
                "subcategory": "Car",
                "detail_category": "General",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_CHILDCARE",
                "category": "Expenses",
                "subcategory": "Childcare",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_CONSULTING_AND_LEGAL",
                "category": "Expenses",
                "subcategory": "Consulting and Legal",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_EDUCATION",
                "category": "Expenses",
                "subcategory": "Education",
                "detail_category": "General",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_INSURANCE",
                "category": "Expenses",
                "subcategory": "Insurance",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_POSTAGE_AND_SHIPPING",
                "category": "Personal Spending",
                "subcategory": "Shipping",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_STORAGE",
                "category": "Expenses",
                "subcategory": "Storage",
                "detail_category": None,
                "budget_amount": -100.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_OTHER_GENERAL_SERVICES",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GENERAL_SERVICES",
                "subcategory_raw": "GENERAL_SERVICES_TECH",
                "category": "Expenses",
                "subcategory": "Tech",
                "detail_category": None,
                "budget_amount": -36.88,
            },
            # GOVERNMENT_AND_NON_PROFIT
            {
                "category_raw": "GOVERNMENT_AND_NON_PROFIT",
                "subcategory_raw": "GOVERNMENT_AND_NON_PROFIT_DONATIONS",
                "category": "Personal Spending",
                "subcategory": "Donations",
                "detail_category": None,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GOVERNMENT_AND_NON_PROFIT",
                "subcategory_raw": "GOVERNMENT_AND_NON_PROFIT_GOVERNMENT_DEPARTMENTS_AND_AGENCIES",
                "category": "Expenses",
                "subcategory": "Govt Departments",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GOVERNMENT_AND_NON_PROFIT",
                "subcategory_raw": "GOVERNMENT_AND_NON_PROFIT_TAX_PAYMENT",
                "category": "Expenses",
                "subcategory": "Financial Planning",
                "detail_category": "Taxes",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "GOVERNMENT_AND_NON_PROFIT",
                "subcategory_raw": "GOVERNMENT_AND_NON_PROFIT_OTHER_GOVERNMENT_AND_NON_PROFIT",
                "category": "Personal Spending",
                "subcategory": "Shopping",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # TRANSPORTATION
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_BIKES_AND_SCOOTERS",
                "category": "Personal Spending",
                "subcategory": "Transportation",
                "detail_category": "Bikes and Scooters",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_GAS",
                "category": "Expenses",
                "subcategory": "Car",
                "detail_category": "Gas",
                # "budget_amount": -100.0,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_PARKING",
                "category": "Personal Spending",
                "subcategory": "Transportation",
                "detail_category": "Parking",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_PUBLIC_TRANSIT",
                "category": "Expenses",
                "subcategory": "Transportation",
                "detail_category": "Public Transit",
                "budget_amount": -132.0,
            },
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_TAXIS_AND_RIDE_SHARES",
                "category": "Personal Spending",
                "subcategory": "Transportation",
                "detail_category": "Taxis",
                # "budget_amount": -50.0,
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_TOLLS",
                "category": "Expenses",
                "subcategory": "Transportation",
                "detail_category": "Tolls",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRANSPORTATION",
                "subcategory_raw": "TRANSPORTATION_OTHER_TRANSPORTATION",
                "category": "Expenses",
                "subcategory": "Transportation",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # TRAVEL
            {
                "category_raw": "TRAVEL",
                "subcategory_raw": "TRAVEL_FLIGHTS",
                "category": "Personal Spending",
                "subcategory": "Travel",
                "detail_category": "Flights",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRAVEL",
                "subcategory_raw": "TRAVEL_LODGING",
                "category": "Personal Spending",
                "subcategory": "Travel",
                "detail_category": "Lodging",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRAVEL",
                "subcategory_raw": "TRAVEL_RENTAL_CARS",
                "category": "Personal Spending",
                "subcategory": "Travel",
                "detail_category": "Rental Cars",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "TRAVEL",
                "subcategory_raw": "TRAVEL_OTHER_TRAVEL",
                "category": "Personal Spending",
                "subcategory": "Travel",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
            # RENT_AND_UTILITIES
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_GAS_AND_ELECTRICITY",
                "category": "Expenses",
                "subcategory": "Utilities",
                "detail_category": "Gas and Electricity",
                "budget_amount": -80.0,
            },
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_INTERNET_AND_CABLE",
                "category": "Expenses",
                "subcategory": "Utilities",
                "detail_category": "Internet and Cable",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_RENT",
                "category": "Expenses",
                "subcategory": "Rent",
                "detail_category": None,
                "budget_amount": -1365.0,
                # "budget_amount": -1400.0,
            },
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_SEWAGE_AND_WASTE_MANAGEMENT",
                "category": "Expenses",
                "subcategory": "Utilities",
                "detail_category": "Sewage and Waste Management",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_TELEPHONE",
                "category": "Expenses",
                "subcategory": "Utilities",
                "detail_category": "Telephone",
                "budget_amount": -8.7,
            },
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_WATER",
                "category": "Expenses",
                "subcategory": "Utilities",
                "detail_category": "Water",
                "budget_amount": 0.0,
            },
            {
                "category_raw": "RENT_AND_UTILITIES",
                "subcategory_raw": "RENT_AND_UTILITIES_OTHER_UTILITIES",
                "category": "Expenses",
                "subcategory": "Utilities",
                "detail_category": "Other",
                "budget_amount": 0.0,
            },
        ]
