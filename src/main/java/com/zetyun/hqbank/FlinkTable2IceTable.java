package com.zetyun.hqbank;

import com.zetyun.hqbank.util.FileUtil;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlinkTable2IceTable {
    private static Logger logger = LoggerFactory.getLogger(FlinkTable2IceTable.class);

    public static void main(String[] args) {

        String hiveUri = "thrift://172.20.29.46:9083";
        String warehouse = "hdfs://172.20.29.46:8020/user/hive/warehouse/";
//        String hiveConfDir = "/etc/hive/conf.cloudera.hive/";
//        String hadoopConfDir = "/etc/hadoop/conf.cloudera.yarn/";
        String hiveConfDir = "D:\\workspace\\iceberg-demo\\config\\hive-conf-46";
        String hadoopConfDir = "D:\\workspace\\iceberg-demo\\config\\hive-conf-46";
        // 读取配置文件
        List<String> topics = YamlUtil.getListByKey("application.yaml", "kafka", "topic");

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(3600000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);
        for (int i = 0; i < topics.size(); i++) {
            // create flink table with kafka topic
            String tableName = topics.get(i);
            String sql = FileUtil.readFile("ddl/kafka/" + tableName + ".sql");

            String bootstrap = YamlUtil.getValueByKey("application.yaml", "kafka", "bootstrap");

            sql = sql.replace("_TOPIC_", tableName).replace("_BOOTSTRAP_", bootstrap);

            // create hive_catalog
            logger.info("create iceberg_catalog now!");
            String catalogName = "iceberg_catalog";
            String createCatalog = "create catalog " + catalogName + " with (\n" +
                    "   'type'='iceberg',\n" +
                    "   'catalog-type'='hive',\n" +
                    "   'uri'='" + hiveUri + "',\n" +
                    "   'hive-conf-dir'='" + hiveConfDir + "',\n" +
                    "   'hadoop-conf-dir'='" + hadoopConfDir + "',\n" +
                    "   'client'='1',\n" +
                    "   'property-version'='2',\n" +
                    "   'warehouse'='" + warehouse + "'" +
                    ")\n";
            logger.info("catalog:{}", createCatalog);
            streamTableEnv.executeSql(createCatalog);

            // create database
            String databaseName = YamlUtil.getValueByKey("application.yaml", "table", "database");
            streamTableEnv.executeSql("create database if not exists " + catalogName + "." + databaseName);
            streamTableEnv.executeSql("create database if not exists " + databaseName);

            String sinkTable = catalogName + "." + databaseName + ".ice_" + tableName;
//            String sinkTable = catalogName + "." + databaseName + ".kafka_" + tableName;
            String sourceTable = databaseName + ".kafka_" + tableName;
            streamTableEnv.executeSql("drop table if exists " + sourceTable);
            streamTableEnv.executeSql("drop table if exists " + sinkTable);

            // create flink table with kafka topic
            sql = sql.replace("_db_", databaseName);
            logger.info("create flink kafka table :{}", sql);
            streamTableEnv.executeSql(sql);

            // create flink table with iceberg
            String iceSql = FileUtil.readFile("ddl/iceberg/" + tableName + ".sql");
            iceSql = iceSql.replace("_CATALOG_", catalogName).replace("_db_", databaseName);
            logger.info("create flink iceberg table :{}", iceSql);
            streamTableEnv.executeSql(iceSql);

            // create sinkTable
            String insertSql = "insert into " + sinkTable + "  select * from " + sourceTable;
            logger.info("insert :{}", insertSql);
            streamTableEnv.executeSql(insertSql);

        }
    }
}
