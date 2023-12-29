package com.zetyun.hqbank;

import com.sun.security.auth.module.Krb5LoginModule;
import com.zetyun.hqbank.service.oracle.OracleService;
import com.zetyun.hqbank.util.YamlUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class FlinkTable2IceTable {
    private static Logger logger = LoggerFactory.getLogger(FlinkTable2IceTable.class);
    public static final String PATH = "D:/conf/windows/application.yaml";

    public static void main(String[] args) {
        // kerberos 认证配置
        String jaasConf = YamlUtil.getValueByKey(PATH, "kerberos", "jaasConf");
        String krb5Conf = YamlUtil.getValueByKey(PATH, "kerberos", "krb5Conf");
        String krb5Keytab = YamlUtil.getValueByKey(PATH, "kerberos", "krb5Keytab");
        String principal = YamlUtil.getValueByKey(PATH, "kerberos", "principal");

        // catalog 配置信息
        String hiveUri = YamlUtil.getValueByKey(PATH, "hadoop", "hiveUri");
        String warehouse = YamlUtil.getValueByKey(PATH, "hadoop", "warehouse");
        String hiveConfDir = YamlUtil.getValueByKey(PATH, "hadoop", "hiveConfDir");
        String hadoopConfDir = YamlUtil.getValueByKey(PATH, "hadoop", "hadoopConfDir");
        // 读取建表语句
        String databaseName = YamlUtil.getValueByKey(PATH, "table", "database");
        List<String> owners = YamlUtil.getListByKey(PATH, "table", "owner");
        // kafka 配置
        String bootstrap = YamlUtil.getValueByKey(PATH, "kafka", "bootstrap");
        // 白名单，如果不为空，则建立所有表的流
        List<String> whiteList = YamlUtil.getListByKey(PATH, "table", "whiteListB");
        String catalogName = YamlUtil.getValueByKey(PATH, "catalog", "iceberg");
        Boolean deleteOldFlinkTable = YamlUtil.getBooleanValueByKey(PATH, "flink", "deleteOldTable");


        logger.info("jaasConf:{}",jaasConf);
        logger.info("krb5Conf:{}",krb5Conf);
        logger.info("krb5Keytab:{}",krb5Keytab);
        logger.info("principal:{}",principal);
        logger.info("bootstrap:{}",bootstrap);
        logger.info("hiveUri:{}",hiveUri);
        logger.info("warehouse:{}",warehouse);
        logger.info("hiveConfDir:{}",hiveConfDir);
        logger.info("hadoopConfDir:{}",hadoopConfDir);
        logger.info("databaseName:{}",databaseName);
        logger.info("catalogName:{}",catalogName);
        logger.info("deleteOldFlinkTable:{}",deleteOldFlinkTable);
        logger.info("owners:{}",owners);
        logger.info("whiteList:{}",whiteList);

//        Configuration conf = new Configuration();
//        conf.setInteger(RestOptions.PORT, 10000);
        // flink 指定 jaas 必须此配置 用于认证
        System.setProperty("java.security.auth.login.config", jaasConf);

        Properties flinkProps = new Properties();
        flinkProps.setProperty("security.kerberos.krb5-conf.path", krb5Conf);
        flinkProps.setProperty("security.kerberos.login.keytab", krb5Keytab);
        flinkProps.setProperty("security.kerberos.login.principal", principal);
        flinkProps.setProperty("security.kerberos.login.contexts", "Client,KafkaClient");
        flinkProps.setProperty("state.backend", "hashmap");

        Configuration flinkConfig = new Configuration();
        flinkConfig.addAllToProperties(flinkProps);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(8000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);

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

            if (CollectionUtils.isNotEmpty(whiteList)) {
                if (!whiteList.contains(tableName)) {
                    continue;
                }
            }

            String sinkTable = catalogName + "." + databaseName + ".ICE_" + tableName;
            String sourceTable = databaseName + ".KAFKA_" + tableName;
            if (deleteOldFlinkTable) {
                streamTableEnv.executeSql("drop table if exists " + sourceTable);
                streamTableEnv.executeSql("drop table if exists " + sinkTable);
            }

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
