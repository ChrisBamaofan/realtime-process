package com.zetyun.hqbank;

import com.zetyun.hqbank.service.oracle.OracleService;
import com.zetyun.hqbank.util.FileUtil;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkTable2IceTable {
    private static Logger logger = LoggerFactory.getLogger(FlinkTable2IceTable.class);
    private static final HashMap<String,HashMap<String,String>> tableNameMap = new HashMap<>();

    public static void main(String[] args) {

        String hiveUri = "thrift://172.20.29.46:9083";
        String warehouse = "hdfs://172.20.29.46:8020/user/hive/warehouse/";
//        String hiveConfDir = "/etc/hive/conf.cloudera.hive/";
//        String hadoopConfDir = "/etc/hadoop/conf.cloudera.yarn/";
        String hiveConfDir = "D:\\workspace\\iceberg-demo\\config\\hive-conf-46";
        String hadoopConfDir = "D:\\workspace\\iceberg-demo\\config\\hive-conf-46";
//        String hiveUri = "thrift://172.20.1.34:9083";
//        String warehouse = "hdfs://172.20.1.34:8020/user/hive/warehouse/";
//        String hiveConfDir = "D:\\workspace\\iceberg-demo\\config\\hive-conf-34";
//        String hadoopConfDir = "D:\\workspace\\iceberg-demo\\config\\hive-conf-34";
        // 读取配置文件
        List<String> topics = YamlUtil.getListByKey("application.yaml", "kafka", "topic");

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 10001);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(10000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);
        String catalogName = "iceberg_catalog_zhj";
        String databaseName = YamlUtil.getValueByKey("application.yaml", "table", "database");
        List<String> owners = YamlUtil.getListByKey("application.yaml", "table", "owner");
        String bootstrap = YamlUtil.getValueByKey("application.yaml", "kafka", "bootstrap");

        HashMap<String, HashMap<String, String>> stringHashMapHashMap = null;
        OracleService oracleTrigger = new OracleService();
        for (int j=0;j< owners.size();j++){
            String owner = owners.get(j);
            stringHashMapHashMap = oracleTrigger.generateKafkaSql(catalogName, databaseName, owner, bootstrap);

        }
//        for (Map.Entry entry:stringHashMapHashMap.entrySet()){}

        for (int i = 0; i < topics.size(); i++) {
            // create flink table with kafka topic
            String tableName = topics.get(i);
            String sql = FileUtil.readFile("ddl/kafka/" + tableName + ".sql");

            sql = sql.replace("_TOPIC_", tableName).replace("_BOOTSTRAP_", bootstrap);

            // create hive_catalog
            logger.info("create iceberg_catalog now!");

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
            streamTableEnv.executeSql("create database if not exists " + catalogName + "." + databaseName);
            streamTableEnv.executeSql("create database if not exists " + databaseName);

            String sinkTable = catalogName + "." + databaseName + ".ICE_" + tableName;
            String sourceTable = databaseName + ".KAFKA_" + tableName;
//            streamTableEnv.executeSql("drop table if exists " + sourceTable);
//            streamTableEnv.executeSql("drop table if exists " + sinkTable);

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

//            streamTableEnv.executeSql("insert into iceberg_catalog_zhj.orcl.ICE_DDS_T01 values(2,'test','2023-12-26 09:37:00')");

        }
//
//        Table table = streamTableEnv.sqlQuery("select * from iceberg_catalog_zhj.orcl.ICE_DDS_T01");
//        streamTableEnv.toAppendStream(table, Row.class).print("iceberg_catalog_zhj.orcl.ICE_DDS_T01 print is:");
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


    }
}
