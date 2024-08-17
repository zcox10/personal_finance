SELECT 
  PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
  COUNTIF(item_id IS NULL) AS null_items,
  COUNTIF(account_id IS NULL) AS null_account_ids,
  COUNTIF(transaction_id IS NULL) AS null_transaction_ids,
  COUNTIF(date_removed IS NULL) AS null_dates_removed,
  COUNT(1) total_rows
FROM `{full_table_name_0d}`