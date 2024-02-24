CREATE TABLE if not exists _CATALOG_._db_.ICE_DDS_T01 (
    C1 decimal
    PRIMARY KEY (`C1`) NOT ENFORCED ) PARTITIONED BY (`C1`) WITH (
    'type'='iceberg',
    'table_type'='iceberg',
    'format-version'='2',
    'engine.hive.enabled' = 'true',
    'write.upsert.enabled'='true',
    'table.exec.sink.not-null-enforcer'='true')


    CREATE TABLE if not exists _CATALOG_._db_.ICE_DEMO ( C1 decimal, PRIMARY KEY (`C1`) NOT ENFORCED )
    PARTITIONED BY (`C1`) WITH ('type'='iceberg', 'table_type'='iceberg',    'format-version'='2',
        'engine.hive.enabled' = 'true',   'write.upsert.enabled'='true',    'table.exec.sink.not-null-enforcer'='true')