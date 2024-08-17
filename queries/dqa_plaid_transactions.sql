WITH
  plaid_transactions AS (
  SELECT 
    item_id,
    account_id,
    transaction_id,
    is_pending,
    `status`,
    transaction_date,
    authorized_date,
    amount,
    currency_code,
    personal_finance_category.primary,
    personal_finance_category.detailed,
    personal_finance_category.confidence_level,
    payment_channel,
    merchant.name,
  FROM `{full_table_name_0d}`
  )
  , null_counts AS (
  SELECT 
    PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
    COUNTIF(item_id IS NULL) AS null_items,
    COUNTIF(account_id IS NULL) AS null_accounts,
    COUNTIF(transaction_id IS NULL) AS null_transaction_ids,
    COUNTIF(is_pending IS NULL) AS null_pendings,
    COUNTIF(`status` IS NULL) AS null_statuses,
    COUNTIF(transaction_date IS NULL) AS null_transaction_dates,
    COUNTIF(amount IS NULL) AS null_amounts,
    COUNTIF(currency_code IS NULL) AS null_currency_codes,
    COUNTIF(primary IS NULL) AS null_primary_categories,
    COUNTIF(detailed IS NULL) AS null_detail_categories,
    COUNTIF(confidence_level IS NULL) AS null_category_confidences,
    COUNTIF(payment_channel IS NULL) AS null_payment_channel,
    COUNTIF(name IS NULL) AS null_merchant_names,
  FROM plaid_transactions  
  )
  , transaction_categories AS (
  SELECT DISTINCT 
    `primary` AS category_raw,
    detailed AS subcategory_raw
  FROM plaid_transactions
  WHERE detailed NOT IN ("TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS")
  )
  , budget_values_categories AS (
  SELECT DISTINCT 
    category_raw,
    subcategory_raw
  FROM `zsc-personal.budget_values.budget_values_*`
  )
  , missing_categories_data AS (
  SELECT
    STRING_AGG(CONCAT(category_raw, " - ", subcategory_raw), "; " ORDER BY CONCAT(category_raw, " - ", subcategory_raw)) AS missing_categories,
    COUNT(DISTINCT CONCAT(category_raw, " - ", subcategory_raw)) AS missing_categories_count,
  FROM transaction_categories t
  LEFT JOIN budget_values_categories b 
  USING (category_raw, subcategory_raw)
  WHERE 
    b.category_raw IS NULL
    AND b.subcategory_raw IS NULL
  )
SELECT *
FROM null_counts
CROSS JOIN missing_categories_data
