-- Temporary function to convert int size bytes into human readable bytes (KB, MB, GB, etc.)
CREATE TEMP FUNCTION format_size_bytes(size_bytes INT64) 
RETURNS STRING 
AS (
  CASE
    WHEN size_bytes < 1024 THEN CONCAT(size_bytes, ' B')
    WHEN size_bytes < 1024 * 1024 THEN CONCAT(ROUND(size_bytes / 1024, 2), ' KB')
    WHEN size_bytes < 1024 * 1024 * 1024 THEN CONCAT(ROUND(size_bytes / 1024 / 1024, 2), ' MB')
    ELSE CONCAT(ROUND(size_bytes / 1024 / 1024 / 1024, 2), ' GB')
  END
);
-- Read table information from personal_finance_tableau and budget_values datasets
-- Use QUALIFY() to filter for the latest partition of each table
SELECT 
  -- order by partition date
  CASE 
    WHEN table_id IN ("temp_plaid_cursors") THEN NULL
    WHEN dataset_id = "personal_finance" THEN SUBSTR(table_id, -8)
    ELSE SUBSTR(table_id, -6)
  END AS partition_date,

  * EXCEPT(dataset_id)
FROM (
  SELECT 
    dataset_id,
    table_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(creation_time))  AS creation_time,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(last_modified_time))  AS last_modified_time,
    row_count,
    format_size_bytes(size_bytes) AS table_size,
  FROM `zsc-personal.personal_finance.__TABLES__`
  UNION ALL
  SELECT 
    dataset_id,
    table_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(creation_time))  AS creation_time,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MILLIS(last_modified_time))  AS last_modified_time,
    row_count,
    format_size_bytes(size_bytes) AS table_size,
  FROM `zsc-personal.budget_values.__TABLES__`
  )
-- If dataset_id is personal_finance, base_table is {table_name}_YYYYMMDD without "YYYYMMDD"; if any other dataset (e.g. budget_values), assume "YYYYMM" partitioning
QUALIFY ROW_NUMBER() OVER (PARTITION BY REGEXP_EXTRACT(table_id, IF(dataset_id IN ("personal_finance"), r'(.*)\d{8}$', r'(.*)\d{6}$')) ORDER BY table_id DESC) = 1
ORDER BY partition_date DESC, table_id