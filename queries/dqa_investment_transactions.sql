SELECT
  PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
  COUNTIF(item_id IS NULL) AS null_items,
  COUNTIF(account_id IS NULL) AS null_accounts,
  COUNTIF(investment_transaction_id IS NULL) AS null_investment_transaction_ids,
  COUNTIF(investment_date IS NULL) AS null_investment_dates,
  COUNTIF(investment_name IS NULL) AS null_investment_names,
  COUNTIF(quantity IS NULL) AS null_quantities,
  COUNTIF(amount IS NULL) AS null_amounts,
  COUNTIF(price IS NULL) AS null_prices,
  COUNTIF(fees IS NULL) AS null_fees,
  COUNTIF(investment_type IS NULL) AS null_investment_types,
  COUNTIF(investment_subtype IS NULL) AS null_investment_subtypes,
  COUNTIF(currency_code IS NULL) AS null_currency_codes,
  COUNT(1) AS total_rows,
FROM
  `{full_table_name_0d}`