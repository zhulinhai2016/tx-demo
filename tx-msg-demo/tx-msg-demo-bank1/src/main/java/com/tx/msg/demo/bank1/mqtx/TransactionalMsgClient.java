package com.tx.msg.demo.bank1.mqtx;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 通过事务保证本地事务于事务消息表写入原子性
 *
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 11:31
 **/
@Slf4j
public abstract class TransactionalMsgClient {
    private static final String MQProducerName = "TransactionMsgProducer";
    private DefaultMQProducer producer;
    private String mqAddr;
    // 数据源配置
    private List<DBDataSource> dbDataSources;
    protected MsgProcessor msgProcessor;
    private MsgStorage msgStorage;
    private Config config;
    private AtomicReference<State> state;
    private List<String> topicLists;

    public TransactionalMsgClient(String mqAddr, List<DBDataSource> dbDataSources, List<String> topicLists, Config config) {
        this.mqAddr = mqAddr;
        this.dbDataSources = dbDataSources;
        this.topicLists = topicLists;
        this.config = config;
        msgStorage = new MsgStorage(dbDataSources, topicLists);
        producer = new DefaultMQProducer(MQProducerName); // producer group
        producer.setNamesrvAddr(this.mqAddr); // mq 地址
        msgProcessor = new MsgProcessor(producer, msgStorage);
        this.config = config;
        state = new AtomicReference<State>(State.CREATE);
    }

    /**
     * @param content 事务消息内容
     * @param topic   主题
     * @param tag
     * @return 事务消息插入库的id
     * @throws RuntimeException spring aop 事务默认配置，如果是抛出RuntimeException才会回滚，如果是Exception不会回滚
     */
    public abstract Long sendMsg(String content, String topic, String tag) throws Exception;

    /**
     * 初始化内部MQ Producer,mysql 连接池，线程池等
     *
     * @throws MQClientException
     */
    public void init() throws MQClientException {
        if (state.get().equals(State.RUNNING)){
            log.info("TransactionMsgClient have inited, return");
            return ;
        }
        log.info("start init mqAddr={} state {} this {}",this.mqAddr,state,this);
        producer.setSendMsgTimeout(config.getSendMsgTimeOut());
        if(config == null){
//            config = new Config();
            log.info("TransactionMsgClient without Config instance, return");
            return;
        }
        try {
            producer.start(); // 启动生产者
            msgProcessor.init(this.config);
            msgStorage.init(this.config);
            log.info("end init success");
        } catch (MQClientException ex) {
            log.error("producer start fail", ex);
            throw ex;
        }
        state.compareAndSet(State.CREATE,State.RUNNING);
    }
    public void close(){
        log.info("start close TransactionMsgClient");
        if (state.compareAndSet(State.RUNNING, State.CLOSED)) {
            msgProcessor.close();
            msgStorage.close();
            producer.shutdown();
        } else
            log.info("state not right {} ", state);
    }
    /**
     *
     * @param con 如果我们拿不到连接，需要暴露出来，让业务方set Connection
     * @param content
     * @param topic
     * @param tag
     * @return
     * @throws Exception
     */
    public Long sendMsg(Connection con, String content, String topic, String tag) throws Exception{
        Long id = null;
        if (!state.get().equals(State.RUNNING)){
            log.error("TransactionMsgClient not Running , please call init function");
            throw new Exception("TransactionMsgClient not Running , please call init function");
        }
        if (content == null || content.isEmpty() || topic == null || topic.isEmpty()){
            log.error("content or topic is null or empty");
            throw new Exception("content or topic is null or empty, notice ");
        }
        try {
            log.debug("insert to msgTable topic {} tag {} Connection {} Autocommit {} ",topic,tag,con,con.getAutoCommit());
            if (con.getAutoCommit()){
                log.error("***** attention not in transaction ***** topic {} tag {} Connection {} Autocommit {} ",topic,tag,con,con.getAutoCommit());
                throw new Exception("connection not in transaction con "+con);
            }
            // 事务消息入库
            Map.Entry<Long,String> idUrlPair = msgStorage.insertMsg(con, content, topic, tag);
            id = idUrlPair.getKey();
            Msg msg = new Msg(id,idUrlPair.getValue());
            // 异步发送消息
            msgProcessor.putMsg(msg);

        } catch (Exception ex) {
            log.error("sendMsg fail topic {} tag {} ",topic,tag, ex);
            throw ex;

        }

        return id;
    }

    public String getMqAddr() {
        return mqAddr;
    }

    public void setMqAddr(String mqAddr) {
        this.mqAddr = mqAddr;
    }

    public List<DBDataSource> getDbDataSources() {
        return dbDataSources;
    }

    public void setDbDataSources(List<DBDataSource> dbDataSources) {
        this.dbDataSources = dbDataSources;
    }

    public Config getConfig() {
        return config;
    }

    /**
     *
     * @param config 连接池，线程，内部时间周期等参数
     */
    public void setConfig(Config config) {
        this.config = config;
    }

}
