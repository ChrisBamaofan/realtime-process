CREATE TABLE if not exists   db.tableA (
    id string,
    C1 decimal
    PRIMARY KEY (`C1`) NOT ENFORCED ) PARTITIONED BY (`C1`) WITH (
    'connector' = 'kafka', 'topic' = 'topic', 'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'g1','scan.startup.mode' = 'earliest-offset','format' = 'debezium-json')