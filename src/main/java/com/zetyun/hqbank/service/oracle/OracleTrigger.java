package com.zetyun.hqbank.service.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author zhaohaojie
 * @date 2023-12-22 14:30
 */
public class OracleTrigger {
    private static Connection connection;
    private static final Logger log = LoggerFactory.getLogger(OracleTrigger.class);

    public static void main(String[] args) {

        OracleTrigger how2ObtainFieldInfoFromJdbc = new OracleTrigger();
        // 第一种方式：执行sql语句获取 select * from user_pop_info where 1 = 2
        how2ObtainFieldInfoFromJdbc.method1();
        // 第二种方式：执行sql语句获取 show create table user_pop_info
//        how2ObtainFieldInfoFromJdbc.method2();
        // 第二种方式：直接从jdbc数据库连接Connection实例中获取
//        how2ObtainFieldInfoFromJdbc.method3();
    }

    private void method1() {

        try{
            PreparedStatement preparedStatement1 = connection.prepareStatement("select * from all_tables where owner = 'DBMGR'");
            ResultSet res  = preparedStatement1.executeQuery();
            while(res.next()){
                System.out.println(
                        "owner: " + res.getString(1) + " tableName: " + res.getString(2)
                );
            }

            PreparedStatement preparedStatement = connection.prepareStatement("select * from DDS.T01 where 1 = 2");
            ResultSetMetaData resultSetMetaData = preparedStatement.executeQuery().getMetaData();

            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {

                log.info("数据库实例名:{}", resultSetMetaData.getCatalogName(i + 1));
                log.info("表名:{}", resultSetMetaData.getTableName(i + 1));
                log.info("java类型:{}", resultSetMetaData.getColumnClassName(i + 1));
                log.info("数据库类型:{}", resultSetMetaData.getColumnTypeName(i + 1));
                log.info("字段名称:{}", resultSetMetaData.getColumnName(i + 1));
                log.info("字段长度:{}", resultSetMetaData.getColumnDisplaySize(i + 1));
                log.info("getColumnType:{}", resultSetMetaData.getColumnType(i + 1));
                log.info("getPrecision:{}", resultSetMetaData.getPrecision(i + 1));
                log.info("getScale:{}", resultSetMetaData.getScale(i + 1));
                log.info("getSchemaName:{}", resultSetMetaData.getSchemaName(i + 1));
                log.info("getScale:{}", resultSetMetaData.getScale(i + 1));
            }
        } catch (Exception e) {
            log.error("method1 error ", e);
        }
    }

    private void method2() {
        try{

            PreparedStatement preparedStatement2 = connection.prepareStatement("show create table user_pop_info");
            ResultSet resultSet2 = preparedStatement2.executeQuery();
            while(resultSet2.next()) {
                String tableName = resultSet2.getString("Table");
                String createTable = resultSet2.getString("Create Table");
                log.info("tableName:{}", tableName);
                log.info("createTable:");
                System.out.println(createTable);
            }
        } catch (Exception e) {
            log.error("method2 error ", e);
        }
    }

    private void method3() {

        try{

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            // 获取所有表
            ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"});
            // 获取指定表
            ResultSet specificResultSet = databaseMetaData.getColumns(null, "%", "user_pop_info", "%");

            String columnName2;
            String columnType2;
            String comment2;
            while(specificResultSet.next()) {

                columnName2 = specificResultSet.getString("COLUMN_NAME");
                columnType2 = specificResultSet.getString("TYPE_NAME");
                comment2 = specificResultSet.getString("REMARKS");
                log.info("COLUMN_NAME:{}", columnName2);
                log.info("TYPE_NAME:{}", columnType2);
                log.info("REMARKS:{}", comment2);
            }

        } catch (Exception e) {
            log.error("method3 error ", e);
        }
    }

//    private static final Connection connection = null;
    static {

        try{
            String jdbcUrl = "jdbc:oracle:thin:@172.20.3.225:1521:orcl";
            String username = "dbmgr";
            String password = "oracle";


            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 连接到数据库
            connection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (Exception e) {
            log.error("autoCodeGeneratorProcess error ", e);
        }

    }
}

