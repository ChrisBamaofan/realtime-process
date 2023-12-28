package com.zetyun.hqbank.service.oracle;

import com.zetyun.hqbank.util.YamlUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static com.zetyun.hqbank.FlinkTable2IceTable.PATH;

/**
 * @author zhaohaojie
 * @date 2023-12-22 14:30
 */
public class OracleService {
    public static final String CONFIG_PATH = "/data/rdx/application.yaml";
    private static Connection connection;
    private static final Logger log = LoggerFactory.getLogger(OracleService.class);

    public static void main(String[] args) {

        OracleService how2ObtainFieldInfoFromJdbc = new OracleService();
        // 第一种方式：执行sql语句获取 select * from user_pop_info where 1 = 2
        how2ObtainFieldInfoFromJdbc.method1();
        // 第二种方式：执行sql语句获取 show create table user_pop_info
//        how2ObtainFieldInfoFromJdbc.method2();
        // 第二种方式：直接从jdbc数据库连接Connection实例中获取
//        how2ObtainFieldInfoFromJdbc.method3();
    }

    // orcl-dds-***  => DDS_T01
    public List<String> getTopicNameByDB(String databaseName, String owner) {
        List<String> list = new ArrayList<>();
        try {
            String sql = "select * from all_tables where owner = '" + owner + "'";
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
                log.info("owner: " + res.getString(1) + " tableName: " + res.getString(2));
            }
        } catch (RuntimeException | SQLException exception) {
            log.error("error occur!", exception);
        }

        return list;
    }

    // DDS_T01, <KAFKA_DDS_T01,kafkaSql> <ICE_DDS_T01,iceSQL>
    public HashMap<String, HashMap<String, String>> generateSql(String catalogName, String database, String owner, String bootstrap) {

        HashMap<String, HashMap<String, String>> result = new HashMap<>();
        try {
            String sql = "select * from all_tables where owner = '" + owner + "'";
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql);
            ResultSet res = preparedStatement1.executeQuery();
            while (res.next()) {
                String tableName = res.getString(2);

                StringBuilder kafkaDDLSql = new StringBuilder();
                kafkaDDLSql.append("CREATE TABLE if not exists ");
                kafkaDDLSql.append(database).append(".KAFKA_").append(owner.toUpperCase(Locale.ROOT));
                kafkaDDLSql.append("_");
                kafkaDDLSql.append(tableName.toUpperCase(Locale.ROOT)).append(" ( ");

                StringBuilder icebergSql = new StringBuilder();
                icebergSql.append("CREATE TABLE if not exists ");
                icebergSql.append(catalogName).append(".").append(database)
                        .append(".ICE_").append(owner.toUpperCase(Locale.ROOT)).append("_")
                        .append(tableName.toUpperCase(Locale.ROOT)).append(" ( ");

                // 获取字段
                String cdcSourceTopic = owner.toUpperCase(Locale.ROOT) + "_" + tableName.toUpperCase(Locale.ROOT);
                HashMap<String, String> sqlResults = getColumns(database, owner, tableName, kafkaDDLSql, icebergSql,
                        cdcSourceTopic, bootstrap);
                result.put(cdcSourceTopic, sqlResults);

            }
        } catch (RuntimeException | SQLException runtimeException) {
            log.error("error", runtimeException);
        }
        return result;
    }

    /**
     * VARCHAR2(n)	STRING
     * CHAR(n)	STRING
     * NUMBER(p, s)	DECIMAL(p, s)
     * DATE	TIMESTAMP
     * TIMESTAMP	TIMESTAMP
     * CLOB	STRING
     * BLOB	BYTES
     * BINARY_FLOAT	FLOAT
     * BINARY_DOUBLE	DOUBLE
     * RAW(n)	BYTES
     * INTERVAL YEAR TO MONTH	INTERVAL_YEAR_MONTH
     * INTERVAL DAY TO SECOND	INTERVAL_DAY_TIME
     *
     * @param database
     * @param owner
     * @param table
     * @param kafkaSql
     * @param iceSql
     * @return
     */
    // DDS_T01, <KAFKA_DDS_T01,kafkaSql> <ICE_DDS_T01,iceSQL>
    public HashMap<String, String> getColumns(String database, String owner, String table,
                                              StringBuilder kafkaSql, StringBuilder iceSql,
                                              String topic, String bootstrap) {
        HashMap<String, String> result = new HashMap<>();
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
            iceSql.append("'type'='iceberg', 'table_type'='iceberg', 'format-version'='2', 'engine.hive.enabled' = 'true', 'write.upsert.enabled'='true','table.exec.sink.not-null-enforcer'='true')");
            String kafkaPrefix = "'connector' = 'kafka', 'topic' = '_TOPIC_', 'properties.bootstrap.servers' = '_BOOTSTRAP_', 'properties.group.id' = 'g1','scan.startup.mode' = 'latest-offset','format' = 'debezium-json')";
            kafkaPrefix = kafkaPrefix.replace("_TOPIC_", topic).replace("_BOOTSTRAP_", bootstrap);
            kafkaSql.append(kafkaPrefix);
            result.put("KAFKA_" + topic, kafkaSql.toString());
            result.put("ICE_" + topic, iceSql.toString());

        } catch (Exception e) {
            log.error("method1 error ", e);
        }
        return result;
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
//            PreparedStatement preparedStatement1 = connection.prepareStatement("select * from all_tables where owner = 'DBMGR'");
//            ResultSet res  = preparedStatement1.executeQuery();
//            while(res.next()){
//                System.out.println(
//                        "owner: " + res.getString(1) + " tableName: " + res.getString(2)
//                );
//            }
            // orcl-dds-t01,T01
            //

            PreparedStatement preparedStatement = connection.prepareStatement("select * from DDS.T_BIG where 1 = 2");
            ResultSetMetaData resultSetMetaData = preparedStatement.executeQuery().getMetaData();

            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {

//                log.info("数据库实例名:{}", resultSetMetaData.getCatalogName(i + 1));
//                log.info("表名:{}", resultSetMetaData.getTableName(i + 1));
                log.info("java类型:{}", resultSetMetaData.getColumnClassName(i + 1));
                log.info("数据库类型:{}", resultSetMetaData.getColumnTypeName(i + 1));
                log.info("字段名称:{}", resultSetMetaData.getColumnName(i + 1));
//                log.info("字段长度:{}", resultSetMetaData.getColumnDisplaySize(i + 1));
//                log.info("getColumnType:{}", resultSetMetaData.getColumnType(i + 1));
//                log.info("getPrecision:{}", resultSetMetaData.getPrecision(i + 1));
//                log.info("getScale:{}", resultSetMetaData.getScale(i + 1));
//                log.info("getSchemaName:{}", resultSetMetaData.getSchemaName(i + 1));
//                log.info("getScale:{}", resultSetMetaData.getScale(i + 1));
            }
        } catch (Exception e) {
            log.error("method1 error ", e);
        }
    }


    static {
        try {
            String url = YamlUtil.getValueByKey(CONFIG_PATH, "oracle", "url");
            String username = YamlUtil.getValueByKey(CONFIG_PATH, "oracle", "username");
            String password = YamlUtil.getValueByKey(CONFIG_PATH, "oracle", "password");
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 连接到数据库
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            log.error("autoCodeGeneratorProcess error ", e);
        }

    }

}

