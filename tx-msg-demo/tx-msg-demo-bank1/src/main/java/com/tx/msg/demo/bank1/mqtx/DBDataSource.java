package com.tx.msg.demo.bank1.mqtx;

import org.apache.ibatis.jdbc.RuntimeSqlException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 14:43
 **/
public class DBDataSource {
    private LinkedList<Connection> connectionPool = new LinkedList<Connection>();

    public DBDataSource() {
        for (int i = 0; i < 5; i++) {
            this.connectionPool.add(GetProxy(creatConnection()));
        }
    }

    /**
     * 手写数据库连接池
     * 创建数据库连接
     * 存数据库连接
     *
     */
    private Connection GetProxy(final Connection connection) {
        Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[]{Connection.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Object value ;
                if (method.getName().equalsIgnoreCase("close")){
                    connectionPool.addLast((Connection) proxy);
                    System.out.println(connectionPool.size());
                    return null;
                } else {
                    value = method.invoke(connection,args);
                }
                System.out.println(connectionPool.size());
                return value;
            }
        });

        return null;
    }

    private Connection creatConnection(){
        try {
           return DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
        } catch (SQLException throwables) {
            throw  new RuntimeSqlException("initionazer DB pool fail");
        }
    }

    public Connection getConnection(){
       return connectionPool.removeFirst();
    }

    public void freeConnection(Connection connection){
        connectionPool.addLast(connection);
    }
}
