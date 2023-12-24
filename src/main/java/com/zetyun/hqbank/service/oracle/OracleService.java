package com.zetyun.hqbank.service.oracle;

import java.sql.*;

/**
 * @author zhaohaojie
 * @date 2023-12-22 14:05
 */
public class OracleService {
    public static void main(String[] args) {
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

