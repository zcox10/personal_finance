WITH
  null_counts AS (
    SELECT
      PARSE_DATE("%Y%m", _TABLE_SUFFIX) AS partition_date,
      COUNTIF(category_raw IS NULL) AS null_category_raw,
      COUNTIF(subcategory_raw IS NULL) AS null_subcategory_raw,
      COUNTIF(category IS NULL) AS null_category,
      COUNTIF(subcategory IS NULL) AS null_subcategory,
    FROM
      `zsc-personal.budget_values.budget_values_*`
    GROUP BY
      1
  ),
  null_counts_summed AS (
    SELECT
      STRING_AGG(
        IF(
          (null_category_raw + null_subcategory_raw + null_category + null_subcategory) > 0,
          CAST(partition_date AS STRING),
          CAST(NULL AS STRING)
        ),
        "; "
        ORDER BY
          partition_date
      ) AS partitions_null,
      SUM(null_category_raw) AS null_category_raw,
      SUM(null_subcategory_raw) AS null_subcategory_raw,
      SUM(null_category) AS null_category,
      SUM(null_subcategory) AS null_subcategory,
    FROM
      null_counts
  ),
  partition_0d AS (
    SELECT
      COUNT(DISTINCT category_raw) AS total_category_raw_0d,
      COUNT(DISTINCT subcategory_raw) AS total_subcategory_raw_0d,
      COUNT(DISTINCT category) AS total_category_0d,
      COUNT(DISTINCT subcategory) AS total_subcategory_0d,
      SUM(budget_amount) AS total_budget_amount_0d,
      COUNT(1) total_rows_0d,
    FROM
      `{full_table_name_0d}`
  ),
  partition_1d AS (
    SELECT
      COUNT(DISTINCT category_raw) AS total_category_raw_1d,
      COUNT(DISTINCT subcategory_raw) AS total_subcategory_raw_1d,
      COUNT(DISTINCT category) AS total_category_1d,
      COUNT(DISTINCT subcategory) AS total_subcategory_1d,
      SUM(budget_amount) AS total_budget_amount_1d,
      COUNT(1) total_rows_1d,
    FROM
      `{full_table_name_1d}`
  ),
  partition_comparisons AS (
    SELECT
      PARSE_DATE("%Y%m", "{table_suffix_0d}") AS partition_0d,
      PARSE_DATE("%Y%m", "{table_suffix_1d}") AS partition_1d,
      total_category_raw_0d,
      total_category_raw_1d,
      (total_category_raw_0d - total_category_raw_1d) AS total_category_raw_diff,
      total_subcategory_raw_0d,
      total_subcategory_raw_1d,
      (total_subcategory_raw_0d - total_subcategory_raw_1d) AS total_subcategory_raw_diff,
      total_category_0d,
      total_category_1d,
      (total_category_0d - total_category_1d) AS total_category_diff,
      total_subcategory_0d,
      total_subcategory_1d,
      (total_subcategory_0d - total_subcategory_1d) AS total_subcategory_diff,
      total_rows_0d,
      total_rows_1d,
      (total_rows_0d - total_rows_1d) AS total_rows_diff,
      ROUND(total_budget_amount_0d, 2) total_budget_amount_0d,
      ROUND(total_budget_amount_1d, 2) total_budget_amount_1d,
      ROUND(((total_budget_amount_0d - total_budget_amount_1d) / ABS(total_budget_amount_1d)) * 100, 2) AS total_budget_amount_pct_chg,
      ROUND(ABS(((total_budget_amount_0d - total_budget_amount_1d) / ABS(total_budget_amount_1d)) * 100), 2) AS total_budget_amount_pct_chg_abs,
    FROM
      partition_0d p
      CROSS JOIN partition_1d
  )
SELECT
  *
FROM
  partition_comparisons
  CROSS JOIN null_counts_summed