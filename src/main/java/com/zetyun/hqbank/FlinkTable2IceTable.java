//package com.zetyun.hqbank;
//
//import com.zetyun.hqbank.bean.flink.FlinkTableMap;
//import com.zetyun.hqbank.service.oracle.OracleService;
//import com.zetyun.hqbank.util.KafkaUtil;
//import com.zetyun.hqbank.util.YamlUtil;
//import com.zetyun.rt.jasyptwrapper.Jasypt;
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamStatementSet;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//
//public class FlinkTable2IceTable {
//    private static Logger logger = LoggerFactory.getLogger(FlinkTable2IceTable.class);
//
//    public static void main(String[] args) throws Exception {
//        // kerberos 认证配置
//        ParameterTool parameters = ParameterTool.fromArgs(args);
//        String userConfigPATH = parameters.get("userConfig");
////        String systemConfigPath = "/opt/flink-on-yarn/conf/systemConfig.yaml";
//
//        String systemConfigPath = "D:/conf/windows/systemConfig.yaml";
//        logger.info("userConfigPath:{},systemConfigPath:{}",userConfigPATH,systemConfigPath);
//
//        List<String> owners = YamlUtil.getListByKey(userConfigPATH, "table", "owner");
//        String bootstrap = YamlUtil.getValueByKey(systemConfigPath, "kafka", "bootstrap");
//        // 白名单，如果不为空，则建立所有表的流
//        List<String> tables = YamlUtil.getListByKey(userConfigPATH, "table", "tableNames");
//        String oracleUri = YamlUtil.getValueByKey(userConfigPATH, "oracle", "url");
//        oracleUri = Jasypt.decrypt(oracleUri);
//        String[] parts = oracleUri.split("/");
//        String databaseName = parts[1];
//
//        List<String> whiteList = KafkaUtil.getKafkaTopicB(databaseName,owners.get(0),tables,"target");
//        String catalogName = YamlUtil.getValueByKey(systemConfigPath, "catalog", "iceberg");
//        Long checkpointInterval = Long.valueOf(YamlUtil.getValueByKey(userConfigPATH, "flink", "checkpointInterval"));
//        String warehouse = YamlUtil.getValueByKey(userConfigPATH, "hadoop", "warehouse");
//
//        String jaasConf = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "jaasConf");
//        String krb5Conf = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "krb5Conf");
//        String krb5Keytab = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "krb5Keytab");
//        String principal = YamlUtil.getValueByKey(systemConfigPath, "kerberos", "principal");
//        String hadoopUserName = YamlUtil.getValueByKey(systemConfigPath, "hadoop", "loginUserName");
//
//        logger.info("jaasConf:{}", jaasConf);
//        logger.info("krb5Conf:{}", krb5Conf);
//        logger.info("krb5Keytab:{}", krb5Keytab);
//        logger.info("principal:{}", principal);
//        logger.info("bootstrap:{}", bootstrap);
//        logger.info("warehouse:{}", warehouse);
//        logger.info("databaseName:{}", databaseName);
//        logger.info("catalogName:{}", catalogName);
//        logger.info("owners:{}", owners);
//        logger.info("hadoopUserName:{}", hadoopUserName);
//        logger.info("checkpointInternal:{}", checkpointInterval);
//
//        // flink 指定 jaas 必须此配置 用于认证
//        System.setProperty("java.security.auth.login.config", jaasConf);
//        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
//
//        Properties flinkProps = new Properties();
//        flinkProps.setProperty("security.kerberos.krb5-conf.path", krb5Conf);
//        flinkProps.setProperty("security.kerberos.login.keytab", krb5Keytab);
//        flinkProps.setProperty("security.kerberos.login.principal", principal);
//        flinkProps.setProperty("security.kerberos.login.contexts", "Client,KafkaClient");
//        flinkProps.setProperty("state.backend", "hashmap");
//
//        Configuration flinkConfig = new Configuration();
//        flinkConfig.addAllToProperties(flinkProps);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
//        env.enableCheckpointing(checkpointInterval);
//
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);
//
//        List<FlinkTableMap> flinkTableMaps = new ArrayList<>();
//        OracleService oracleTrigger = new OracleService();
//        for (String owner : owners) {
//            flinkTableMaps = oracleTrigger.generateSql(catalogName, databaseName, owner, bootstrap, tables, userConfigPATH);
//        }
//
//        // create hive_catalog
//        logger.info("==> create iceberg_catalog now!");
//
//        String createCatalog = "create catalog " + catalogName + " with (\n" +
//                "   'type'='iceberg',\n" +
//                "   'catalog-type'='hadoop',\n" +
//                "   'client'='5',\n" +
//                "   'property-version'='2',\n" +
//                "   'warehouse'='" + warehouse + "'" +
//                ")\n";
//        logger.info("==> catalog:{}", createCatalog);
//        streamTableEnv.executeSql(createCatalog);
//
//        // create database
//        streamTableEnv.executeSql("create database if not exists " + catalogName + "." + databaseName);
//        streamTableEnv.executeSql("create database if not exists " + databaseName);
//
//        StreamStatementSet statementSet = streamTableEnv.createStatementSet();
//        for (FlinkTableMap bean: flinkTableMaps) {
//            String sinkTopicName = bean.getTopicName();
//            String kafkaSql = bean.getKafkaSql();
//            String iceSql = bean.getIceSql();
//
//            if (CollectionUtils.isNotEmpty(whiteList)) {
//                if (!whiteList.contains(sinkTopicName)) {
//                    continue;
//                }
//            }
//            logger.info("==> sinkTopicName:{}", sinkTopicName);
//            logger.info("==> kafkaSql:{}", kafkaSql);
//            logger.info("==> iceSql:{}", iceSql);
//
//            String sourceTable = databaseName + "."+bean.getKafkaTableName();
//            String sinkTable = catalogName + "." + databaseName + "." + bean.getIceTableName();
//
//            // create flink table with kafka topic
//            logger.info("==> create flink table with kafka connector:{}", kafkaSql);
//            streamTableEnv.executeSql(kafkaSql);
//
//            // create flink table with iceberg
//            logger.info("==> create flink table with iceberg connector:{}", iceSql);
//            streamTableEnv.executeSql(iceSql);
//
//            // create sinkTable
//            String insertSql = "insert into " + sinkTable + "  select * from " + sourceTable;
//            logger.info("==> insert :{}", insertSql);
//            statementSet.addInsertSql(insertSql);
//        }
//        statementSet.execute();
//    }
//}
