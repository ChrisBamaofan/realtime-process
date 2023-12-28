package com.zetyun.hqbank;

import com.zetyun.hqbank.service.oracle.OracleService;
import com.zetyun.hqbank.util.FileUtil;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zetyun.hqbank.DDS2FlinkCDC.whiteList;

public class FlinkTable2IceTable {
    private static Logger logger = LoggerFactory.getLogger(FlinkTable2IceTable.class);
    private static final HashMap<String, HashMap<String, String>> tableNameMap = new HashMap<>();
    public static final List<String> whiteList = Arrays.asList(new String[]{"DDS_T_ZHJ2"});
    public static final String PATH  = "/data/rdx/application.yaml";
    public static void main(String[] args) {

        String hiveUri = YamlUtil.getValueByKey(PATH, "hadoop", "hiveUri");
        String warehouse = YamlUtil.getValueByKey(PATH, "hadoop", "warehouse");
        String hiveConfDir = YamlUtil.getValueByKey(PATH, "hadoop", "hiveConfDir");
        String hadoopConfDir = YamlUtil.getValueByKey(PATH, "hadoop", "hadoopConfDir");

        // 读取配置文件
//        Configuration conf = new Configuration();
//        conf.setInteger(RestOptions.PORT, 10000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(9000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);
        String catalogName = "iceberg_catalog";
        String databaseName = YamlUtil.getValueByKey(PATH, "table", "database");
        List<String> owners = YamlUtil.getListByKey(PATH, "table", "owner");
        String bootstrap = YamlUtil.getValueByKey(PATH, "kafka", "bootstrap");

        HashMap<String, HashMap<String, String>> sqlMap = null;
        OracleService oracleTrigger = new OracleService();
        for (int j = 0; j < owners.size(); j++) {
            String owner = owners.get(j);
            sqlMap = oracleTrigger.generateSql(catalogName, databaseName, owner, bootstrap);
        }

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

        for (Map.Entry entry : sqlMap.entrySet()) {
            String tableName = (String) entry.getKey();
            HashMap<String, String> value = (HashMap<String, String>) entry.getValue();
            String kafkaSql = value.get("KAFKA_" + tableName);
            String iceSql = value.get("ICE_" + tableName);
            logger.info("kafkaSql:{},iceSql:{},tableName:{}", kafkaSql, iceSql, tableName);
//            if (!whiteList.contains(tableName)) {// todo delete line
//                continue;
//            }
            String sinkTable = catalogName + "." + databaseName + ".ICE_" + tableName;
            String sourceTable = databaseName + ".KAFKA_" + tableName;
            streamTableEnv.executeSql("drop table if exists " + sourceTable);
            streamTableEnv.executeSql("drop table if exists " + sinkTable);

            // create flink table with kafka topic
            logger.info("create flink table with kafka connector:{}", kafkaSql);
            streamTableEnv.executeSql(kafkaSql);

            // create flink table with iceberg
            logger.info("create flink table with iceberg connector:{}", iceSql);
            streamTableEnv.executeSql(iceSql);

            // create sinkTable
            String insertSql = "insert into " + sinkTable + "  select * from " + sourceTable;
            logger.info("insert :{}", insertSql);
            streamTableEnv.executeSql(insertSql);

        }


    }
}
