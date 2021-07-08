package com.tx.msg.demo.bank1.data.pool.impl;

import com.tx.msg.demo.bank1.data.DBUtil;
import com.tx.msg.demo.bank1.data.pool.DbPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据库连接池实现类
 * @Author: ZhuLinHai
 * @Date: 2021/7/7 17:51
 **/
public class DbPoolImpl implements DbPool {

    /**
     * 活跃连接数
     */
    private LinkedBlockingQueue<Connection> idleConnectPool;

    /**
     * 活跃连接数
     */
    private LinkedBlockingQueue<Connection> activeConnectPool;

    /**
     * 正在使用的连接数
     */
    private AtomicInteger activeSize = new AtomicInteger();

    /**
     * 最大连接数
     */
    private final int maxSize;

    public DbPoolImpl(int maxSize) {
        this.maxSize = maxSize;
        // 初始化连接池
        init();
    }

    @Override
    public void init() {
        idleConnectPool = new LinkedBlockingQueue<>();
        activeConnectPool=new LinkedBlockingQueue<>();
    }


    @Override
    public Connection getConnection() {
        Connection connection = idleConnectPool.poll();
        if (connection != null){
            // 如果有连接就放入 active连接池中
            activeConnectPool.offer(connection);
            System.out.println("获取到连接");
            return connection;
        }
        // 如果没有获取连接，则新建联级
        if (activeSize.get() < maxSize){
            // 通过 activeSize.incrementAndGet() <= maxSize 这个判断
            // 解决 if(activeSize.get() < maxSize) 存在的线程安全问题
            if (activeSize.getAndIncrement() <= maxSize){
                // 创建连接
                connection = DBUtil.createConnection();
                activeConnectPool.offer(connection);
                System.out.println("获取到连接");
                return connection;
            }
        }
        // 如果空闲池中连接数达到maxSize， 则阻塞等待归还连接
        System.out.println("排队等待连接");
        try {
            connection = idleConnectPool.poll(10000, TimeUnit.MILLISECONDS);
            if (connection == null) {
                System.out.println("等待超时");
                throw new RuntimeException("等待连接超时");
            }
            System.out.println("等待到了一个连接");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return connection;
    }

    @Override
    public void releaseConnection(Connection con) {
        if (con != null){
            activeConnectPool.remove(con);
            idleConnectPool.offer(con);
        }
    }

    @Override
    public void destroy() {

    }

    /**
     *  定时对连接进行健康检查
     *  注意：只能对idle连接池中的连接进行健康检查，
     *  不可以对busyConnectPool连接池中的连接进行健康检查，因为它正在被客户端使用;
     *
     */
//    @Scheduled(fixedDelay = 60 * 1000)
    public void check(){
        for (int i = 0; i < activeSize.get(); i++) {
            Connection connection = idleConnectPool.poll();
            try {
                boolean valid = connection.isValid(2000);
                if (!valid){
                    // 连接失效则创建一个新连接
                    connection = DBUtil.createConnection();
                }
                idleConnectPool.offer(connection);// 放进一个可用的连接
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
