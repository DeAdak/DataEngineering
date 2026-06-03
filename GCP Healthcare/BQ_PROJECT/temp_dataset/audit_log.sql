CREATE TABLE IF NOT EXISTS `avd-databricks-demo`.temp_dataset.audit_log (
  datasource      STRING,
  tablename       STRING,
  load_timestamp  TIMESTAMP,
  loadtype        STRING,
  record_count    INT64,
  status          STRING
)
PARTITION BY DATE(load_timestamp); -- Optimizes cost and performance
