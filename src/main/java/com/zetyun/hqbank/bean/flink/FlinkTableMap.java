package com.zetyun.hqbank.bean.flink;

/**
 * 这个类包装 topic
 * sql
 * table名
 *
 * @author zhaohaojie
 * @date 2024-02-01 22:29
 */
// target-orcl-dds-t_zhj2, <kafka_target-orcl-dds-t_zhj2,kafkaSql> <ice_target-orcl-dds-t_zhj2,iceSQL>

public class FlinkTableMap {
    /**target-orcl-dds-t_zhj2*/
    String topicName;
    /**kafka_orcl_dds_t_zhj2*/
    String kafkaTableName;
    /**ice_orcl_dds_t_zhj2*/
    String iceTableName;
    /**create table if not exists kafka_orcl_dds_t_zhj2 ();*/
    String kafkaSql;
    /**create table if not exists ice_orcl_dds_t_zhj2 ();*/
    String iceSql;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getKafkaTableName() {
        return kafkaTableName;
    }

    public void setKafkaTableName(String kafkaTableName) {
        this.kafkaTableName = kafkaTableName;
    }

    public String getIceTableName() {
        return iceTableName;
    }

    public void setIceTableName(String iceTableName) {
        this.iceTableName = iceTableName;
    }

    public String getKafkaSql() {
        return kafkaSql;
    }

    public void setKafkaSql(String kafkaSql) {
        this.kafkaSql = kafkaSql;
    }

    public String getIceSql() {
        return iceSql;
    }

    public void setIceSql(String iceSql) {
        this.iceSql = iceSql;
    }
}

