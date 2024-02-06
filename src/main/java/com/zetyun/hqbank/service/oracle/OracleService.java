package com.zetyun.hqbank.service.oracle;

import com.zetyun.hqbank.bean.flink.FlinkTableMap;
import com.zetyun.hqbank.util.YamlUtil;
import com.zetyun.rt.jasyptwrapper.Jasypt;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.zetyun.hqbank.DDS2FlinkCDC.processData;


/**
 * @author zhaohaojie
 * @date 2023-12-22 14:30
 */
public class OracleService {

    private static Connection connection;
    private static final Logger log = LoggerFactory.getLogger(OracleService.class);
    /**
     *
     * {"CHAR1":"3","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}
     *
     *
     * {"CHAR1":"3","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}
     * {"CHAR1":"4","CHAR2":"3","CHAR3":"3tt","DATE_1":"2024-03-03 00:00:00","CHAR4":null,"REAL1":null,"NUM1":null,"NUM2":null,"INT1":null,"FLOAT1":null}
     *
     * */
//    private static String processData(String input,String schema) {
    public static void main(String[] args) {
        String insert="{\"scn\":88125961,\"tms\":\"2024-02-06 10:18:06\",\"xid\":\"11.7.8102\",\"payload\":{\"op\":\"c\",\"schema\":{\"owner\":\"DDS\",\"table\":\"TEST2\"},\"row\":1,\"rid\":\"AAAZUBAAEAAIllEAAG\",\"after\":{\"CHAR1\":\"3\",\"CHAR2\":\"3\",\"CHAR3\":\"3tt\",\"DATE_1\":\"2024-03-03 00:00:00\",\"CHAR4\":null,\"REAL1\":null,\"NUM1\":null,\"NUM2\":null,\"INT1\":null,\"FLOAT1\":null}}}";
        String update = "{\"scn\":88127297,\"tms\":\"2024-02-06 10:39:12\",\"xid\":\"11.15.8101\",\"payload\":{\"op\":\"u\",\"schema\":{\"owner\":\"DDS\",\"table\":\"TEST2\"},\"row\":1,\"rid\":\"AAAZUBAAEAAIllEAAG\",\"before\":{\"CHAR1\":\"3\",\"CHAR2\":\"3\",\"CHAR3\":\"3tt\",\"DATE_1\":\"2024-03-03 00:00:00\",\"CHAR4\":null,\"REAL1\":null,\"NUM1\":null,\"NUM2\":null,\"INT1\":null,\"FLOAT1\":null},\"after\":{\"CHAR1\":\"4\",\"CHAR2\":\"3\",\"CHAR3\":\"3tt\",\"DATE_1\":\"2024-03-03 00:00:00\",\"CHAR4\":null,\"REAL1\":null,\"NUM1\":null,\"NUM2\":null,\"INT1\":null,\"FLOAT1\":null}}}";

//        processData(update,"");
        processData(insert,"");
//        OracleService how2ObtainFieldInfoFromJdbc = new OracleService();
//        // 第一种方式：执行sql语句获取 select * from user_pop_info where 1 = 2
//        how2ObtainFieldInfoFromJdbc.method1();
    }

    // 组装 作业A 从这些topic中拿 数据
    // orcl-dds-t_zhj2
    public List<String> getTopicNameByDB(String userConfigPath,String databaseName, String owner) {
        getConnection(userConfigPath);
        List<String> list = new ArrayList<>();
        try {
            String sql = "SELECT owner, table_name FROM DBA_TABLES WHERE OWNER ='"+owner+"' ORDER BY owner, table_name";

            PreparedStatement preparedStatement1 = connection.prepareStatement(sql);
            ResultSet res = preparedStatement1.executeQuery();
            while (res.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append(databaseName.toLowerCase(Locale.ROOT));
                sb.append("-");
                sb.append(res.getString(1).toLowerCase(Locale.ROOT));
                sb.append("-");
                sb.append(res.getString(2).toLowerCase(Locale.ROOT));
                list.add(sb.toString());
            }
        } catch (RuntimeException | SQLException exception) {
            log.error("error occur!", exception);
        }

        return list;
    }

    public List<FlinkTableMap> generateSql(String catalogName, String database, String owner,
                                           String bootstrap, List<String> whiteList, String path) {
        getConnection(path);
        List<FlinkTableMap> result = new ArrayList<>();
        try {

            String sql = "SELECT owner, table_name FROM DBA_TABLES WHERE OWNER ='"+owner+"' ORDER BY owner, table_name";
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql);
            ResultSet res = preparedStatement1.executeQuery();

            database = database.toLowerCase(Locale.ROOT);
            owner = owner.toLowerCase(Locale.ROOT);
            while (res.next()) {
                String tableName = res.getString(2);

                if (CollectionUtils.isNotEmpty(whiteList)) {
                    if (!whiteList.contains(tableName.toLowerCase(Locale.ROOT))
                    && !whiteList.contains(tableName.toUpperCase(Locale.ROOT))
                    && !whiteList.contains(tableName)
                    ) {
                        continue;
                    }
                }

                tableName = tableName.toLowerCase(Locale.ROOT);
                FlinkTableMap flinkTableMap = new FlinkTableMap();
                String kafkaTableName = "kafka_"+database+"_"+owner+"_"+tableName;
                flinkTableMap.setKafkaTableName(kafkaTableName);
                String iceTableName = "ice_"+database+"_"+owner+"_"+tableName;
                flinkTableMap.setIceTableName(iceTableName);
                String newKafkaTopicName = "target-"+database+"-"+owner+"-"+tableName;
                flinkTableMap.setTopicName(newKafkaTopicName);

                StringBuilder kafkaDDLSql = new StringBuilder();
                kafkaDDLSql.append("CREATE TABLE if not exists ");
                kafkaDDLSql.append(database).append(".kafka_").append(database).append("_").append(owner);
                kafkaDDLSql.append("_");
                kafkaDDLSql.append(tableName).append("( auto_md5_id string,");

                StringBuilder icebergSql = new StringBuilder();
                icebergSql.append("CREATE TABLE if not exists ");
                icebergSql.append(catalogName).append(".").append(database)
                        .append(".ice_").append(database).append("_").append(owner).append("_")
                        .append(tableName).append("( auto_md5_id string,");

                // 获取字段
                flinkTableMap = getColumns(database, owner, tableName, kafkaDDLSql, icebergSql,newKafkaTopicName.toLowerCase(Locale.ROOT), bootstrap,flinkTableMap);
                result.add(flinkTableMap);
            }
        } catch (RuntimeException | SQLException runtimeException) {
            log.error("error", runtimeException);
        }
        return result;
    }

    public FlinkTableMap getColumns(String userConfigPath, String owner, String table,
                                    StringBuilder kafkaSql, StringBuilder iceSql,
                                    String sinkTopic, String bootstrap,FlinkTableMap flinkTableMap) {
        getConnection(userConfigPath);
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from " + owner + "." + table + " where 1 = 2");
            ResultSetMetaData resultSetMetaData = preparedStatement.executeQuery().getMetaData();
            String uniqueIdColumnName = "";
            String firstColumnName = "";

            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                String columnClassName = resultSetMetaData.getColumnClassName(i + 1);
                String columnName = resultSetMetaData.getColumnName(i + 1);
                if (StringUtils.isEmpty(firstColumnName)) {
                    firstColumnName = columnName;
                }
                if (columnClassName.contains("java.lang.String")) {
                    if (StringUtils.isEmpty(uniqueIdColumnName)) {
                        uniqueIdColumnName = columnName;
                    }
                }
                String type = getColumnType(columnClassName);
                kafkaSql.append(columnName).append(" ").append(type).append(",");
                iceSql.append(columnName).append(" ").append(type).append(",");
            }

            if (StringUtils.isEmpty(uniqueIdColumnName)) {
                uniqueIdColumnName = firstColumnName;
            }

            String key = "PRIMARY KEY (`_KEY_`) NOT ENFORCED ) PARTITIONED BY (`_KEY_`) WITH (";
            key = key.replace("_KEY_", uniqueIdColumnName);
            kafkaSql.append(key);
            iceSql.append(key);
            iceSql.append("'type'='iceberg', 'table_type'='iceberg', 'format-version'='2', 'engine.hive.enabled' = 'true', 'write.upsert.enabled'='true','table.exec.sink.not-null-enforcer'='drop')");
            String kafkaPrefix = "'connector' = 'kafka', 'topic' = '_TOPIC_', 'properties.bootstrap.servers' = '_BOOTSTRAP_', 'properties.sasl.kerberos.service.name' = 'kafka','properties.sasl.mechanism' = 'GSSAPI','properties.security.protocol' = 'SASL_PLAINTEXT','properties.group.id' = 'g1','scan.startup.mode' = 'latest-offset','format' = 'debezium-json')";
            kafkaPrefix = kafkaPrefix.replace("_TOPIC_", sinkTopic).replace("_BOOTSTRAP_", bootstrap);
            kafkaSql.append(kafkaPrefix);
            // target-orcl-dds-t_zhj2, <kafka_target-orcl-dds-t_zhj2,kafkaSql> <ice_target-orcl-dds-t_zhj2,iceSQL>
            flinkTableMap.setKafkaSql(kafkaSql.toString());
            flinkTableMap.setIceSql(iceSql.toString());

        } catch (Exception e) {
            log.error("method1 error ", e);
        }
        return flinkTableMap;
    }

    public String getColumnType(String columnClassName) {
        if (columnClassName.contains("java.lang.String")) {
            return "String";
        }
        if (columnClassName.contains("java.math.BigDecimal")) {
            return "decimal";
        }
        if (columnClassName.contains("java.sql.Timestamp")) {
            return "timestamp";
        }
        if (columnClassName.toLowerCase(Locale.ROOT).contains("timestamp")) {
            return "timestamp";
        }
        if (columnClassName.contains("Float")) {
            return "Float";
        }
        if (columnClassName.contains("Double")) {
            return "Double";
        }
        if (columnClassName.contains("OracleClob")
                || columnClassName.contains("OracleBlob")
                || columnClassName.contains("OracleNClob")) {
            return "String";
        }
        if (columnClassName.contains("INTERVALYM") || columnClassName.contains("INTERVALDS")) {
            return "timestamp";
        }
        if (columnClassName.contains("[B")) {
            return "RAW";
        }
        // 增加对应的类型
        return "String";

    }


    private void method1() {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from DDS.T_BIG where 1 = 2");
            ResultSetMetaData resultSetMetaData = preparedStatement.executeQuery().getMetaData();

            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                log.info("java类型:{}", resultSetMetaData.getColumnClassName(i + 1));
                log.info("数据库类型:{}", resultSetMetaData.getColumnTypeName(i + 1));
                log.info("字段名称:{}", resultSetMetaData.getColumnName(i + 1));
            }
        } catch (Exception e) {
            log.error("method1 error ", e);
        }
    }

    public void getConnection(String path){
        try {
            if (connection != null){
                return;
            }
            String url = YamlUtil.getValueByKey(path, "oracle", "url");
            url = Jasypt.decrypt(url);
            String username = YamlUtil.getValueByKey(path, "oracle", "username");
            username = Jasypt.decrypt(username);
            String password = YamlUtil.getValueByKey(path, "oracle", "password");
            password = Jasypt.decrypt(password);
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 连接到数据库
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            log.error("autoCodeGeneratorProcess error ", e);
        }
    }


}

