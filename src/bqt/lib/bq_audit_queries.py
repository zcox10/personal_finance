audit_modifier_query = '''
WITH
src_da AS (
  SELECT
    timestamp,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.tableCopy.destinationTable.datasetId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load.destinationTable.datasetId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId) AS dataset,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.tableCopy.destinationTable.tableId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load.destinationTable.tableId,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId) AS `table`,
    protopayload_auditlog.authenticationInfo.principalEmail AS modifier
  FROM `{project}.bigquery_audit_logs.cloudaudit_googleapis_com_data_access_*`
  WHERE severity = 'INFO'),
da AS (
  SELECT *
  FROM src_da
  WHERE
    dataset IS NOT NULL
    AND table IS NOT NULL
    AND COALESCE(SUBSTR(dataset, 1, 1), '_') != '_' ),
grp_da AS (
  SELECT dataset, table, MAX(timestamp) AS max_timestamp
  FROM da
  GROUP BY 1, 2),
final_da AS (
  SELECT da.dataset, da.table,
         da.modifier AS last_modifier,
         da.timestamp AS max_timestamp
  FROM da
  INNER JOIN grp_da
  USING (dataset, table)
  WHERE da.timestamp = grp_da.max_timestamp),
src_act AS (
   SELECT
    timestamp,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.tableInsertResponse.resource.tableName.datasetId,
      protopayload_auditlog.servicedata_v1_bigquery.tableUpdateResponse.resource.tableName.datasetId) AS dataset,
    COALESCE(protopayload_auditlog.servicedata_v1_bigquery.tableInsertResponse.resource.tableName.tableId,
      protopayload_auditlog.servicedata_v1_bigquery.tableUpdateResponse.resource.tableName.tableId) AS table,
    protopayload_auditlog.authenticationInfo.principalEmail AS modifier
  FROM
    `{project}.bigquery_audit_logs.cloudaudit_googleapis_com_activity_*`
  WHERE
    protopayload_auditlog.authenticationInfo.principalEmail != 'org-level-write@bq-retention.iam.gserviceaccount.com'
  ),
act AS (
  SELECT *
  FROM src_act
  WHERE
    dataset IS NOT NULL
    AND table IS NOT NULL
    AND COALESCE(SUBSTR(dataset, 1, 1), '_') != '_' ),
grp_act AS (
  SELECT dataset, table, MAX(timestamp) AS max_timestamp
  FROM act
  GROUP BY 1, 2),
final_act AS (
  SELECT act.dataset, act.table,
         act.modifier AS last_modifier,
         act.timestamp AS max_timestamp
  FROM act
  INNER JOIN grp_act
  USING (dataset, table)
  WHERE act.timestamp = grp_act.max_timestamp)
SELECT
  COALESCE(da.dataset, act.dataset) as dataset,
  COALESCE(da.table, act.table) as `table`,
  COALESCE(da.last_modifier, act.last_modifier) as last_modifier,
  COALESCE(da.max_timestamp, act.max_timestamp) as max_timestamp
FROM final_da AS da
FULL OUTER JOIN final_act AS act USING (dataset, table)
'''

audit_access_query = '''
SELECT
  tables.datasetId AS dataset,
  tables.tableId AS `table`,
  MAX(timestamp) AS last_accessed
FROM `{project}.bigquery_audit_logs.cloudaudit_googleapis_com_data_access_*`,
UNNEST (protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables) as tables
WHERE tables.projectId = '{project}' AND severity = 'INFO'
GROUP BY 1, 2
'''
