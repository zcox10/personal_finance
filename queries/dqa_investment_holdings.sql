WITH
  partition_0d AS (
    SELECT
      COUNTIF(item_id IS NULL) AS null_items,
      COUNTIF(account_id IS NULL) AS null_accounts,
      COUNTIF(institution_price IS NULL) AS null_institution_prices,
      COUNTIF(institution_price_date IS NULL) AS null_institution_price_dates,
      COUNTIF(institution_value IS NULL) AS null_institution_values,
      COUNTIF(currency_code IS NULL) AS null_currency_codes,
      COUNTIF(quantity IS NULL) AS null_quantities,
      COUNTIF(security.security_id IS NULL) AS null_security_ids,
      COUNTIF(security.currency_code IS NULL) AS null_security_currency_codes,
      COUNTIF(security.is_cash_equivalent IS NULL) AS null_cash_equivalents,
      COUNTIF(security.name IS NULL) AS null_security_names,
      COUNTIF(security.type IS NULL) AS null_security_types,
      COUNT(DISTINCT item_id) AS total_items_0d,
      COUNT(DISTINCT account_id) AS total_accounts_0d,
      SUM(institution_value) AS total_investment_value_0d
    FROM
      `{full_table_name_0d}`
  ),
  partition_1d AS (
    SELECT
      COUNT(DISTINCT item_id) AS total_items_1d,
      COUNT(DISTINCT account_id) AS total_accounts_1d,
      SUM(institution_value) AS total_investment_value_1d
    FROM
      `{full_table_name_1d}`
  )
SELECT
  PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
  PARSE_DATE("%Y%m%d", "{table_suffix_1d}") AS partition_1d,
  p.* EXCEPT (total_items_0d, total_accounts_0d, total_investment_value_0d),
  total_items_0d,
  total_items_1d,
  (total_items_0d - total_items_1d) AS total_items_diff,
  total_accounts_0d,
  total_accounts_1d,
  (total_accounts_0d - total_accounts_1d) AS total_accounts_diff,
  ROUND(total_investment_value_0d, 2) total_investment_value_0d,
  ROUND(total_investment_value_1d, 2) total_investment_value_1d,
  ROUND(((total_investment_value_0d - total_investment_value_1d) / ABS(total_investment_value_1d)) * 100, 2) AS total_investment_value_pct_chg,
  ROUND(
    ABS(((total_investment_value_0d - total_investment_value_1d) / ABS(total_investment_value_1d)) * 100),
    2
  ) AS total_investment_value_pct_chg_abs,
FROM
  partition_0d p
  CROSS JOIN partition_1d