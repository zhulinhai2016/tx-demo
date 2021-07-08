package com.tx.msg.demo.bank1.mqtx;

import com.tx.msg.demo.bank1.data.pool.DbPool;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;

import java.sql.Connection;
import java.util.List;

/**
 * @Author: WuYunXi
 * @Date: 2021/7/5 0005 22:51
 **/
@Slf4j
public class MybatisTransactionMsgClient extends TransactionalMsgClient{

    private SqlSessionTemplate sessionTemplate;

    public MybatisTransactionMsgClient(SqlSessionTemplate sessionTemplate, String mqAddr, DbPool dbDataSources, List<String> topicLists) {
        super(mqAddr, dbDataSources, topicLists, new Config());
        this.sessionTemplate = sessionTemplate;
    }
    public MybatisTransactionMsgClient(SqlSessionTemplate sessionTemplate, String mqAddr, DbPool dbDataSources, List<String> topicLists, Config config) {
        super(mqAddr, dbDataSources, topicLists, config);
        this.sessionTemplate = sessionTemplate;
    }
    public MybatisTransactionMsgClient(SqlSessionFactory sqlSessionFactory, String mqAddr, DbPool dbDataSources, List<String> topicLists, Config config){
        super(mqAddr,dbDataSources,topicLists,config);
        //this.sqlSessionFactory = sqlSessionFactory;
        try {
            this.sessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
        } catch (Exception e) {
            //  Auto-generated catch block
            log.error("get sqlSessionFactory fail",e);
        }
    }
    public MybatisTransactionMsgClient(SqlSessionFactory sqlSessionFactory,String mqAddr,DbPool dbDataSources,List<String> topicLists){
        this(sqlSessionFactory, mqAddr, dbDataSources, topicLists,new Config());
    }

    /**
     * 客户端发送消息
     * @param content 事务消息内容
     * @param topic   主题
     * @param tag
     * @return
     * @throws Exception
     */
    @Override
    public Long sendMsg(String content, String topic, String tag) throws Exception {
        Long id = null;
        try {
            // 拿到当前连接
            Connection con = sessionTemplate.getConnection();
            id = super.sendMsg(con,content,topic,tag);
            return id;
        } catch (Exception ex) {
            log.error("sendMsg fail topic {} tag {} ", topic,tag,ex);
            throw new RuntimeException(ex);

        }
    }

    public SqlSessionTemplate getSessionTemplate() {
        return sessionTemplate;
    }

    public void setSessionTemplate(SqlSessionTemplate sessionTemplate) {
        this.sessionTemplate = sessionTemplate;
    }

    public void setHistoryMsgStoreTime(int historyMsgStoreTime) {
        this.getConfig().setHistoryMsgStoreTime(historyMsgStoreTime);
    }

    public int getHistoryMsgStoreTime() {
        return this.getConfig().getHistoryMsgStoreTime();
    }

}
