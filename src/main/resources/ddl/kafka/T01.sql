CREATE TABLE if not exists   _db_.kafka_T01 (
    C1 decimal,
    C2 STRING,
    C3 timestamp,
    PRIMARY KEY (`C1`) NOT ENFORCED
 ) PARTITIONED BY (`C1`)
 WITH (
    'connector' = 'kafka',
    'topic' = '_TOPIC_',
    'properties.bootstrap.servers' = '_BOOTSTRAP_',
    'properties.group.id' = 'g1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
)