package com.zetyun.hqbank.service.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

/**
 * @author zhaohaojie
 * @date 2023-12-22 14:30
 */
public class OracleService {
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
    public List<String> getTopicNameByDB(String databaseName, String owner){
        List<String> list = new ArrayList<>();
        try{
            String sql = "select * from all_tables where owner = '"+owner+"'";
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql);
            ResultSet res  = preparedStatement1.executeQuery();
            while(res.next()){
                StringBuilder sb = new StringBuilder();
                sb.append(databaseName.toLowerCase(Locale.ROOT));
                sb.append("-");
                sb.append(res.getString(1).toLowerCase(Locale.ROOT));
                sb.append("-");
                sb.append(res.getString(2).toLowerCase(Locale.ROOT));
                list.add(sb.toString());
                log.info("owner: " + res.getString(1) + " tableName: " + res.getString(2));
            }
        }catch (RuntimeException | SQLException exception){
            log.error("error occur!",exception);
        }

        return list;
    }

    // DDS_T01, <KAFKA_DDS_T01,kafkaSql> <ICE_DDS_T01,iceSQL>
    public HashMap<String,HashMap<String,String>> generateKafkaSql(String catalogName,String database,String owner){

        try{
            String sql = "select * from all_tables where owner = '"+owner+"'";
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql);
            ResultSet res  = preparedStatement1.executeQuery();
            while(res.next()){
                String tableName = res.getString(2);

                StringBuilder kafkaDDLSql = new StringBuilder();
                kafkaDDLSql.append("CREATE TABLE if not exists ");
                kafkaDDLSql.append(database).append("KAFKA_").append(owner.toUpperCase(Locale.ROOT));
                kafkaDDLSql.append(tableName.toUpperCase(Locale.ROOT));

                StringBuilder icebergSql = new StringBuilder();
                icebergSql.append("CREATE TABLE if not exists ");
                icebergSql.append(catalogName).append(".").append(database).append(".ICE_").append(tableName.toUpperCase(Locale.ROOT));

                // 获取字段
                kafkaDDLSql = getColumns(database,owner,tableName,kafkaDDLSql);


            }
        }catch (RuntimeException | SQLException runtimeException){
            log.error("error",runtimeException);
        }
return null;
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
    public HashMap<String,String> getColumns(String database,String owner,String table,StringBuilder kafkaSql,StringBuilder iceSql){
        try{

            PreparedStatement preparedStatement = connection.prepareStatement("select * from "+owner+"."+table+" where 1 = 2");
            ResultSetMetaData resultSetMetaData = preparedStatement.executeQuery().getMetaData();

            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                String columnName = resultSetMetaData.getColumnName(i + 1);
                Integer columnLength = resultSetMetaData.getColumnDisplaySize(i + 1);
                Integer columnType = resultSetMetaData.getColumnType(i + 1);
//                if (columnType){
//
//                }

            }
        } catch (Exception e) {
            log.error("method1 error ", e);
        }
        return new HashMap<>();
    }

    private void method1() {

        try{
//            PreparedStatement preparedStatement1 = connection.prepareStatement("select * from all_tables where owner = 'DBMGR'");
//            ResultSet res  = preparedStatement1.executeQuery();
//            while(res.next()){
//                System.out.println(
//                        "owner: " + res.getString(1) + " tableName: " + res.getString(2)
//                );
//            }
            // orcl-dds-t01,T01
            //

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

    public void getCreateSql(){
        // JDBC连接信息
        String jdbcUrl = "jdbc:oracle:thin:@172.20.3.225:1521:orcl";
        String username = "dbmgr";
        String password = "oracle";

        Connection connection = null;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 连接到数据库
            connection = DriverManager.getConnection(jdbcUrl, username, password);

            // 获取数据库元数据
            DatabaseMetaData metadata = connection.getMetaData();

            // 指定表名和模式（如果有的话）
            // select * from all_tables where owner = 'DDS';
            //
            String tableName = "T01";
            String schema = "DDS";

            // 获取表的元数据
            ResultSet resultSet = metadata.getTables(null, schema, tableName, null);

            while (resultSet.next()) {
                // 获取表的建表语句
                String createTableSQL = resultSet.getString("SQL");
                System.out.println("Create Table SQL for " + tableName + ":\n" + createTableSQL);
            }

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // 关闭数据库连接
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

