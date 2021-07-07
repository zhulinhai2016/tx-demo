package com.tx.msg.demo.bank1.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 数据库工具类
 * @Author: ZhuLinHai
 * @Date: 2021/7/7 18:11
 **/
public class DBUtil {
    private String driverClass = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://localhost:3306/testdb";
    private String username = "root";
    private String password = "123456";
    private AtomicBoolean inited = new AtomicBoolean(false);

    /**
     * 创建数据库连接
     * @return
     */
    public Connection createConnection(){
        Connection connection = null;
        try {
            if (!inited.get()){
                if (inited.getAndSet(true)){
                    Class.forName(driverClass);
                }
            }
            connection = DriverManager.getConnection(url,username,password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void executeInsert(Connection connection,String insertSql) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate(insertSql);
    }
}
