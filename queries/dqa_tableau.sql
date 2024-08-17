WITH
  partition_0d AS (
  SELECT  
    -- null: all category
    COUNTIF(partition_date IS NULL) AS null_partition_dates,
    COUNTIF(metric_category IS NULL) AS null_metric_categories,
    COUNTIF(transaction_date IS NULL) AS null_transaction_dates,
    COUNTIF(transaction_month IS NULL) AS null_transaction_months,
    COUNTIF(actual_amount IS NULL) AS null_actual_amounts,

    -- null: transactions_agg
    COUNTIF(item_id IS NULL AND metric_category NOT IN ("TRANSACTIONS_AGG")) AS null_item_ids,
    COUNTIF(account_name IS NULL AND metric_category NOT IN ("TRANSACTIONS_AGG")) AS null_account_names,
    COUNTIF(account_subname IS NULL AND metric_category NOT IN ("TRANSACTIONS_AGG")) AS null_account_subnames,
    COUNTIF(account_type IS NULL AND metric_category NOT IN ("TRANSACTIONS_AGG")) AS null_account_types,
    COUNTIF(budget_amount IS NULL AND metric_category IN ("TRANSACTIONS_AGG")) AS null_budget_amounts,
    COUNTIF(transactions_count IS NULL AND metric_category IN ("TRANSACTIONS_AGG")) AS null_transaction_counts,
    COUNTIF(currency_code IS NULL AND metric_category NOT IN ("TRANSACTIONS_AGG")) AS null_currency_codes,

    -- null: tranasactions
    COUNTIF(transaction_type IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_transaction_types,
    COUNTIF(category_raw IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_category_raws,
    COUNTIF(subcategory_raw IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_subcategory_raws,
    COUNTIF(category_updated IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_category_updated,
    COUNTIF(subcategory_updated IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_subcategory_updated,
    COUNTIF(payment_channel IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_payment_channels,
    COUNTIF(merchant_name IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_merchant_names,
    COUNTIF(name_raw IS NULL AND metric_category IN ("TRANSACTIONS")) AS null_name_raw,

    -- null: transactions and transactions_agg
    COUNTIF(category IS NULL AND metric_category IN ("TRANSACTIONS", "TRANSACTIONS_AGG")) AS null_categories,
    COUNTIF(subcategory IS NULL AND category NOT IN ("Balance") AND metric_category IN ("TRANSACTIONS", "TRANSACTIONS_AGG")) AS null_subcategories,
    COUNTIF(final_category IS NULL AND metric_category IN ("TRANSACTIONS", "TRANSACTIONS_AGG")) AS null_final_categories,
    COUNTIF(full_category IS NULL AND metric_category IN ("TRANSACTIONS", "TRANSACTIONS_AGG")) AS null_full_categories,

    -- null: transactions_agg and accounts
    COUNTIF(account_id IS NULL AND metric_category NOT IN ("TRANSACTIONS_AGG", "ACCOUNTS")) AS null_account_ids,

    -- null: investments
    COUNTIF(institution_price IS NULL AND metric_category IN ("INVESTMENTS")) AS null_institution_prices,
    COUNTIF(quantity IS NULL AND metric_category IN ("INVESTMENTS")) AS null_quantities,
    COUNTIF(security_type IS NULL AND metric_category IN ("INVESTMENTS")) AS null_security_types,
    COUNTIF(security_name IS NULL AND metric_category IN ("INVESTMENTS")) AS null_security_names,

    -- null: transactions and investments
    COUNTIF(transaction_id IS NULL AND metric_category IN ("TRANSACTIONS", "INVESTMENTS")) AS null_transaction_ids,

    -- null: duplicates. These are not null, but since they should add to 0, using these to create a null_alert
    (
    SELECT COUNT(1) 
    FROM (
      SELECT CONCAT(transaction_date, " - ", transaction_id)
      FROM `{full_table_name_0d}`
      WHERE transaction_date IS NOT NULL AND transaction_id IS NOT NULL
      GROUP BY 1
      HAVING COUNT(1) > 1
      )
  ) AS null_duplicate_transaction_ids_per_date, -- identifying if there are any duplicate transaction_id's per transaction_date

  (
    SELECT COUNT(1) 
    FROM (
      SELECT CONCAT(metric_category, " - ", transaction_date, " - ", item_id, IF(account_id IS NULL, "", CONCAT(" - ", account_id)))
      FROM `{full_table_name_0d}`
      WHERE 
        transaction_date IS NOT NULL
        AND item_id IS NOT NULL
        AND metric_category = "ACCOUNTS"
      GROUP BY 1
      HAVING COUNT(1) > 1
      )
  ) AS null_duplicate_account_ids_per_date, -- identifying if there are any duplicate item_id/account_id's per transaction_date

    -- total counts: partition_1d
    COUNT(DISTINCT metric_category) AS total_metric_categories_0d, -- compare partition_1d
    COUNT(DISTINCT item_id) AS total_items_0d, -- compare partition_1d
    COUNT(DISTINCT account_id) AS total_accounts_0d, -- compare partition_1d
    SUM(IF(metric_category = "ACCOUNTS", actual_amount, 0)) AS total_account_summed_value_0d, -- compare partition_1d
    SUM(actual_amount) AS total_actual_amount_0d, -- compare partition_1d

    -- total counts: financial_accounts
    SUM(IF(metric_category = "ACCOUNTS" AND partition_date = PARSE_DATE("%Y%m%d", "{table_suffix_0d}"), actual_amount, 0)) AS total_account_value_0d, -- compare financial_accounts
  FROM `{full_table_name_0d}`
  )
  , partition_1d AS (
  SELECT 
    COUNT(DISTINCT metric_category) AS total_metric_categories_1d,
    COUNT(DISTINCT item_id) AS total_items_1d,
    COUNT(DISTINCT account_id) AS total_accounts_1d,
    SUM(IF(metric_category = "ACCOUNTS", actual_amount, 0)) AS total_account_summed_value_1d,
    SUM(actual_amount) AS total_actual_amount_1d,
  FROM `{full_table_name_1d}`
  )
  , financial_accounts AS (
  SELECT SUM(IF(account_type = "credit", balance.current * -1, balance.current)) AS total_account_value_financial_accounts
  FROM `{financial_accounts_table_0d}`
  )
SELECT 
  PARSE_DATE("%Y%m%d", "{financial_accounts_table_suffix_0d}") AS partition_fin_accts,
  PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
  PARSE_DATE("%Y%m%d", "{table_suffix_1d}") AS partition_1d,

  p.* EXCEPT(
    total_metric_categories_0d,
    total_items_0d,
    total_accounts_0d,
    total_account_summed_value_0d,
    total_actual_amount_0d,
    total_account_value_0d
  ),

  total_metric_categories_0d,
  total_metric_categories_1d,
  (total_metric_categories_0d - total_metric_categories_1d) AS total_metric_categories_diff,

  total_items_0d,
  total_items_1d,
  (total_items_0d - total_items_1d) AS total_items_diff,

  total_accounts_0d,
  total_accounts_1d,
  (total_accounts_0d - total_accounts_1d) AS total_accounts_diff,

  ROUND(total_account_value_0d, 2) AS total_account_value_0d,
  ROUND(total_account_value_financial_accounts, 2) AS total_account_value_fin_accts,
  ROUND(total_account_value_0d - total_account_value_financial_accounts, 2) AS total_account_value_diff,

  ROUND(total_account_summed_value_0d, 2) AS total_account_summed_value_0d,
  ROUND(total_account_summed_value_1d, 2) AS total_account_summed_value_1d,
  ROUND(((total_account_summed_value_0d - total_account_summed_value_1d) / ABS(total_account_summed_value_1d)) * 100, 2)  AS total_account_summed_value_pct_chg,
  ROUND(ABS(((total_account_summed_value_0d - total_account_summed_value_1d) / ABS(total_account_summed_value_1d)) * 100), 2)  AS total_account_summed_value_pct_chg_abs,

  ROUND(total_actual_amount_0d, 2) AS total_actual_amount_0d,
  ROUND(total_actual_amount_1d, 2) AS total_actual_amount_1d,
  ROUND(((total_actual_amount_0d - total_actual_amount_1d) / ABS(total_actual_amount_1d)) * 100, 2)  AS total_actual_amount_pct_chg,
  ROUND(ABS(((total_actual_amount_0d - total_actual_amount_1d) / ABS(total_actual_amount_1d)) * 100), 2)  AS total_actual_amount_pct_chg_abs,

FROM partition_0d p
CROSS JOIN partition_1d
CROSS JOIN financial_accounts
