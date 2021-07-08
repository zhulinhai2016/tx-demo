package com.tx.msg.demo.bank1.data;

import com.tx.msg.demo.bank1.data.pool.DbPool;
import com.tx.msg.demo.bank1.data.pool.impl.DbPoolImpl;
import com.tx.msg.demo.bank1.mqtx.Config;
import com.tx.msg.demo.bank1.mqtx.MybatisTransactionMsgClient;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/7 18:23
 **/
@Configuration
@AutoConfigureAfter(Config.class)
public class DbPoolConfig {

    @Bean
    public DbPool dbPool(){
        return new DbPoolImpl(5);
    }

    @Bean
    @ConditionalOnMissingBean
    public MybatisTransactionMsgClient transactionalMsgClient(DbPool dbPool, SqlSessionTemplate sessionTemplate, Config config){

        return new MybatisTransactionMsgClient(sessionTemplate,"",dbPool,null,config);
    }
}
