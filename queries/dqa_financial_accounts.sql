WITH
  partition_0d AS (
    SELECT
      COUNTIF(item_id IS NULL) AS null_items,
      COUNTIF(
        account_source = "PLAID"
        AND account_id IS NULL
      ) AS null_plaid_accounts,
      COUNTIF(account_name IS NULL) AS null_account_names,
      COUNTIF(account_type IS NULL) AS null_account_types,
      COUNTIF(account_subtype IS NULL) AS null_account_subtypes,
      COUNTIF(account_source IS NULL) AS null_account_source,
      COUNTIF(
        account_source = "PLAID"
        AND institution_id IS NULL
      ) AS null_institution_id,
      COUNTIF(
        account_source = "PLAID"
        AND institution_name IS NULL
      ) AS null_institution_name,
      COUNTIF(balance.current IS NULL) AS null_balance,
      COUNTIF(balance.currency_code IS NULL) AS null_currency_code,
      COUNTIF(
        account_source = "PLAID"
        AND update_type IS NULL
      ) AS null_plaid_update_type,
      COUNTIF(
        account_source = "PLAID"
        AND ARRAY_LENGTH(products) = 0
      ) AS null_plaid_products,
      COUNTIF(
        account_source = "PLAID"
        AND ARRAY_LENGTH(billed_products) = 0
      ) AS null_plaid_billed_products,
      COUNT(DISTINCT item_id) AS total_items_0d,
      COUNT(1) AS total_accounts_0d,
      SUM(balance.current) AS total_account_value_0d
    FROM
      `{full_table_name_0d}`
  ),
  partition_1d AS (
    SELECT
      COUNT(DISTINCT item_id) AS total_items_1d,
      COUNT(1) AS total_accounts_1d,
      SUM(balance.current) AS total_account_value_1d
    FROM
      `{full_table_name_1d}`
  )
SELECT
  PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
  PARSE_DATE("%Y%m%d", "{table_suffix_1d}") AS partition_1d,
  p.* EXCEPT (total_items_0d, total_accounts_0d, total_account_value_0d),
  total_items_0d,
  total_items_1d,
  (total_items_0d - total_items_1d) AS total_items_diff,
  total_accounts_0d,
  total_accounts_1d,
  (total_accounts_0d - total_accounts_1d) AS total_accounts_diff,
  ROUND(total_account_value_0d, 2) total_account_value_0d,
  ROUND(total_account_value_1d, 2) total_account_value_1d,
  ROUND(((total_account_value_0d - total_account_value_1d) / ABS(total_account_value_1d)) * 100, 2) AS total_account_value_pct_chg,
  ROUND(ABS(((total_account_value_0d - total_account_value_1d) / ABS(total_account_value_1d)) * 100), 2) AS total_account_value_pct_chg_abs,
FROM
  partition_0d p
  CROSS JOIN partition_1d