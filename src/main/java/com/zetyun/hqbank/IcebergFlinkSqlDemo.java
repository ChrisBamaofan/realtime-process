package com.zetyun.hqbank;

import com.zetyun.hqbank.util.FileUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.zetyun.hqbank.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IcebergFlinkSqlDemo {
    private static Logger logger = LoggerFactory.getLogger(IcebergFlinkSqlDemo.class);

    public static void main(String[] args) {

        String hiveUri = "thrift://172.20.29.46:9083";
        String warehouse = "hdfs://172.20.29.46:8020/user/hive/warehouse/";
        String hiveConfDir = "/etc/hive/conf.cloudera.hive/";
        String hadoop_conf_dir = "/etc/hadoop/conf.cloudera.yarn/";
        // 读取配置文件
        List<String> topics = YamlUtil.getListByKey("application.yaml", "kafka", "topic");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // create flink table with kafka topic
        //todo check point 要设置的 大一点
        // 合并小文件的问题，
        String tableName = "t_1";
        String sql = FileUtil.readFile(tableName+".sql");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);
        logger.info("执行sql:{}",sql);
        streamTableEnv.executeSql(sql);

        // create hive_catalog
        String catalogName = "iceberg_catalog";
        streamTableEnv.executeSql("create catalog "+catalogName+" with (\n" +
                "   'type'='iceberg',\n" +
                "   'catalog-type'='hive',\n" +
                "   'uri'='" + hiveUri + "',\n" +
                "   'hive-conf-dir'='" + hiveConfDir + "',\n" +
                "   'hadoop-conf-dir'='" + hadoop_conf_dir + "',\n" +
                "   'client'='1',\n" +
                "   'property-version'='2',\n" +
                "   'warehouse'='" + warehouse + "'" +
                ")\n");

        // create database
        String databaseName = YamlUtil.getValueByKey("application.yaml", "table", "database");
        streamTableEnv.executeSql("create database if not exists "+catalogName+"."+databaseName);


        streamTableEnv.executeSql("create table if not exists iceberg_catalog.sinkTable(" +
                "id int, " +
                "name String" +
                ") with ('type'='iceberg', 'upsert'='true','table_type'='iceberg', 'format-version'='2', 'engine.hive.enabled' = 'true')"
        );
        // create 语句 的到 sinkTable todo iceberg 建表 配置 append模式

        streamTableEnv.executeSql("insert into sinkTable select xxxx from sourceTable");


    }


}
