CREATE TABLE if not exists _CATALOG_._db_.ice_T01 (
    C1 decimal,
    C2 STRING,
    C3 timestamp,
    PRIMARY KEY (`C1`) NOT ENFORCED
) PARTITIONED BY (`C1`)
WITH (
    'type'='iceberg',
    'table_type'='iceberg',
    'format-version'='2',
    'engine.hive.enabled' = 'true',
    'write.upsert.enabled'='true',
    'table.exec.sink.not-null-enforcer'='true'
)