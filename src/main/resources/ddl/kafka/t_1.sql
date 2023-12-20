CREATE TABLE topic_products ( id BIGINT, NAME STRING, description STRING, weight DECIMAL ( 10, 2 ) ) WITH (
    'connector' = 'kafka',
    'topic' = 'dds_cdc_sink',
    'properties.bootstrap.servers' = '172.20.29.5:19092',
    'properties.group.id' = 'g1',
    'format' = 'debezium-json'
)