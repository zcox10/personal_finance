SELECT
  PARSE_DATE("%Y%m%d", "{table_suffix_0d}") AS partition_0d,
  COUNTIF(item_id IS NULL) AS null_items,
  COUNTIF(next_cursor IS NULL) AS null_cursors,
  COUNT(1) total_rows
FROM
  `{full_table_name_0d}`