package com.tx.msg.demo.bank1.data.pool;

import java.sql.Connection;

/**
 * 数据库连接池
 * @Author: ZhuLinHai
 * @Date: 2021/7/7 17:49
 **/
public interface DbPool {

    /**
     * 初始化连接
     */
    void init();

    /**
     * 获取一个数据库连接
     * @return
     */
    Connection getConnection();

    /**
     * 释放一个连接
     * @param con
     */
    void releaseConnection(Connection con);

    /**
     * 销毁一个连接
     */
    void destroy();
}
