package com.tx.msg.demo.bank1.data;

import com.tx.msg.demo.bank1.data.pool.DbPool;
import com.tx.msg.demo.bank1.data.pool.impl.DbPoolImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/7 18:23
 **/
@Configuration
public class DbPoolConfig {

    @Bean
    public DbPool dbPool(){
        return new DbPoolImpl(5);
    }
}
