package com.tx.msg.demo.bank1.mqtx;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 主要思想，一个按照创建时间排序的msg Queue，一个按照下次超时时间排序的时间轮队列
 * 由定时线程扫描时间轮，重新放入msg Queue，超过6次，直接丢弃，由离线的定时扫描做重试
 * 只从DB里面查到了，才说明事务提交了
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 14:18
 **/
public class MsgProcessor {
    private static final int getTimeoutMs = 100;
    private static final int[] timeOutData = new int[]{0,5,10,25,50,100}; //5ms,10ms,25ms,50ms,100ms
    private static final int maxDealTime = 6;
    private static final int limitNum = 50;
    private static final int maxDealNumOneTime = 2000;
    private static final int timeWheelPeriod = 5;//5ms
    private static boolean envNeedLock = false;
    //private static boolean isTestEnv = false;
    private static final String canExeKey = "CanExe";
    private static final String deletePrefix = "delete_";
    private static final String selectPrefix = "select_";
    private static final int holdLockTime = 60 ;//60s
    private static final int sanboxTimes = 3;
    private static final int checkLockInitDelay = 10;
    private static final Logger log = LoggerFactory.getLogger(MsgProcessor.class);
    private PriorityBlockingQueue<Msg> msgQueue;
    private ExecutorService exeService;

    //private ThreadPoolExecutor msgExecutor;
    private PriorityBlockingQueue<Msg> timeWheelQueue;
    private ScheduledExecutorService scheService;
    private AtomicReference<State> state;
    private DefaultMQProducer producer;
    private MsgStorage msgStorage;
    private Config config;
    private ZZLockClient zzLockClient; // 分布式锁
    private volatile  boolean holdLock = true;
    private String lockKey = "defaultTransKey";
    private ServerType serverType = null;


    public MsgProcessor(DefaultMQProducer producer, MsgStorage msgStorage) {
        this.producer = producer;
        this.msgStorage = msgStorage;
        msgQueue = new PriorityBlockingQueue<Msg>(5000, new Comparator<Msg>() {
            @Override
            public int compare(Msg o1, Msg o2) {
                long diff = o1.getCreateTime() - o2.getCreateTime();
                if (diff > 0)
                    return 1;
                else if (diff < 0 )
                    return -1;
                return 0;
            }
        });
        timeWheelQueue = new PriorityBlockingQueue<Msg>(1000, new Comparator<Msg>() {
            @Override
            public int compare(Msg o1, Msg o2) {

                long diff = o1.getNextExpireTime() - o2.getNextExpireTime();
                if(diff>0)
                    return 1;
                else if(diff<0)
                    return -1;
                return 0;
            }
        });
        state = new AtomicReference<State>(State.CREATE);
    }

    /**
     * init 阶段，config才是ok
     * @param config
     */
    public void init(Config config){
        if(state.get().equals(State.RUNNING)){
            log.info("Msg Processor have inited return");
            return ;
        }
        log.info("MsgProcessor init start");
        state.compareAndSet(State.CREATE, State.RUNNING);
        this.serverType = Util.getServerType();
        String[] defaultEtcd = null;
        if(config.getEtcdHosts() != null && config.getEtcdHosts().length >=1 ){
            envNeedLock = true;
            defaultEtcd = config.getEtcdHosts();
        }
        log.info("serverType {} envNeedLock {} etcdhosts {}",serverType,envNeedLock,defaultEtcd);
        this.config = config;
        if(ServerType.Sandbox.equals(serverType)){
            this.config.setDeleteTimePeriod(this.config.getDeleteTimePeriod() * sanboxTimes);
            this.config.setSchedScanTimePeriod(this.config.getSchedScanTimePeriod() * sanboxTimes);
        }
        exeService = Executors.newFixedThreadPool(config.getThreadNum(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                // TODO Auto-generated method stub
                Thread thread = new Thread(r,"MsgProcessorThread");
                return thread;
            }
        });
        scheService = Executors.newScheduledThreadPool(config.getSchedThreadNum(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                // TODO Auto-generated method stub
                Thread thread = new Thread(r,"MsgScheduledThread");
                return thread;

            }
        });

        for (int i = 0;i<config.getThreadNum();i++){
            MsgProcessorRunnable runnable = new MsgProcessorRunnable();
            exeService.submit(runnable);
        }
        scheService.scheduleAtFixedRate(new MsgTimeWheelRunnable(),timeWheelPeriod,timeWheelPeriod,TimeUnit.MILLISECONDS);
        scheService.scheduleAtFixedRate(new DeleteMsgRunnable(),config.deleteTimePeriod,config.deleteTimePeriod,TimeUnit.SECONDS);
        scheService.scheduleAtFixedRate(new SchedScanMsgRunnable(),config.schedScanTimePeriod,config.schedScanTimePeriod,TimeUnit.SECONDS);
        scheService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // TODO Auto-generated method stub
                log.info("stats info msgQueue size {} timeWheelQueue size {}",msgQueue.size(),timeWheelQueue.size());
            }
        },20,config.statsTimePeriod,TimeUnit.SECONDS);
    }
    public void close(){
        exeService.shutdown();
        scheService.shutdown();
        producer.shutdown();
    }

    protected void putMsg(Msg msg){
        msgQueue.put(msg);
    }

    private static Message buildMsg(final MsgInfo msgInfo) throws UnsupportedEncodingException {
        String topic = msgInfo.getTopic();
        String tag = msgInfo.getTag();
        String content = msgInfo.getContent();
        String id = msgInfo.getId()+"";
        Message msg = new Message(topic,tag,id,content.getBytes("UTF-8")); /* Message body */
        String header = String.format("{\"topic\":\"%s\",\"tag\":\"%s\",\"id\":\"%s\",\"createTime\":\"%s\"}",topic,tag,id,System.currentTimeMillis());
        msg.putUserProperty("MQHeader", header);
        return msg;
    }


    class MsgProcessorRunnable implements Runnable {

        @Override
        public void run() {
            // TODO Auto-generated method stub
            while (true) {
                if(!state.get().equals(State.RUNNING))
                    break;
                //log.trace("msg Processor start run");
                Msg msg = null;
                try {
                    try{
                        msg = msgQueue.poll(getTimeoutMs, TimeUnit.MILLISECONDS);
                    }catch(InterruptedException ex){

                    }
                    if (msg == null)
                        continue;
                    log.trace("poll msg {}", msg);
                    int dealedTime = msg.getHaveDealedTimes() + 1;
                    msg.setHaveDealedTimes(dealedTime);
                    MsgInfo msgInfo = msgStorage.getMsgById(msg);
                    // 这里我们不知道是否事务已经提交，所以需要从DB里面拿
                    log.trace("getMsgInfo from DB {}", msgInfo);
                    if(msgInfo == null ){
                        if(dealedTime < maxDealTime){
                            long nextExpireTime = System.currentTimeMillis() + timeOutData[dealedTime];
                            msg.setNextExpireTime(nextExpireTime);
                            // 加入时间轮检查队列
                            timeWheelQueue.put(msg);
                            log.trace("put msg in timeWhellQueue {} ",msg);
                        }
                    }else{
                        Message mqMsg = buildMsg(msgInfo);
                        SendResult result = producer.send(mqMsg);
                        log.info("msgId {} topic {} tag {} sendMsg result {}", msgInfo.getId(),mqMsg.getTopic(),mqMsg.getTags(),result);
                        if (null == result || result.getSendStatus() != SendStatus.SEND_OK) {
                            if (dealedTime < maxDealTime) {
                                long nextExpireTime = System.currentTimeMillis() + timeOutData[dealedTime];
                                msg.setNextExpireTime(nextExpireTime);
                                timeWheelQueue.put(msg);
                            }
                        } else if (result.getSendStatus() == SendStatus.SEND_OK) {
                            // 修改数据库的状态
                            int res = msgStorage.updateMsgStatus(msg);
                            log.trace("msgId {} updateMsgStatus success res {}", msgInfo.getId(), res);
                        }
                    }

                }catch (Exception e) {
                    // TODO Auto-generated catch block
                    log.error("MsgProcessor deal msg fail",e);
                }
            }
        }

    }
    class MsgTimeWheelRunnable implements Runnable {

        @Override
        public void run() {
            // TODO Auto-generated method stub
            try{
                if(state.get().equals(State.RUNNING)){
                    //log.trace("msgTimeWheelRun start run");
                    long cruTime = System.currentTimeMillis();
                    Msg msg = timeWheelQueue.peek();
                    // 拿出来的时候有可能还没有超时
                    while(msg != null && msg.getNextExpireTime() <= cruTime){
                        msg = timeWheelQueue.poll();
                        log.trace("timeWheel poll msg ,return to msgQueue {}",msg);
                        msgQueue.put(msg);// 重新放进去
                        msg = timeWheelQueue.peek();
                    }
                }
            }catch(Exception ex){
                log.error("pool timequeue error",ex);
            }
        }
    }

    //后台任务，从数据库中删除已发送的消息
    class DeleteMsgRunnable implements Runnable{
        @Override
        public void run() {
            // TODO Auto-generated method stub
            if(state.get().equals(State.RUNNING)){
                log.trace("DeleteMsg start run");
                try{
                    HashMap<String, DataSource> data = msgStorage.getDataSourcesMap();
                    Collection<DataSource> dataSrcList = data.values();
                    Iterator<DataSource> it = dataSrcList.iterator();
                    while(it.hasNext()){
                        DataSource dataSrc = it.next();
                        boolean canExe = holdLock;
                        if(canExe){
                            log.info("DeleteMsgRunnable run ");
                            int count = 0 ;
                            int num = config.deleteMsgOneTimeNum;
                            while(num == config.deleteMsgOneTimeNum && count < maxDealNumOneTime){
                                try {
                                    num = msgStorage.deleteSendedMsg(dataSrc, config.deleteMsgOneTimeNum);
                                    count += num;
                                } catch (SQLException e) {
                                    // TODO Auto-generated catch block
                                    log.error("deleteSendedMsg fail ",e);
                                }
                            }
                        }
                    }
                }catch(Exception ex){
                    log.error("delete Run error ",ex);
                }
            }
        }

    }

    class SchedScanMsgRunnable implements Runnable{

        @Override
        public void run() {
            if (state.get().equals(State.RUNNING)) {
                log.trace("SchedScanMsg start run");
                HashMap<String, DataSource> data = msgStorage.getDataSourcesMap();
                Collection<DataSource> dataSrcList = data.values();
                Iterator<DataSource> it = dataSrcList.iterator();
                while (it.hasNext()) {
                    DataSource dataSrc = it.next();
                    boolean canExe = holdLock;
                    if(canExe){
                        log.info("SchedScanMsgRunnable run");
                        int num = limitNum;
                        int count = 0;
                        while (num == limitNum && count < maxDealNumOneTime) {
                            try {
                                List<MsgInfo> list = msgStorage.getWaitingMsg(dataSrc, limitNum);
                                num = list.size();
                                if (num > 0)
                                    log.trace("scan db get msg size {} ", num);
                                count += num;
                                for (MsgInfo msgInfo : list) {
                                    try {
                                        Message mqMsg = buildMsg(msgInfo);

                                        //向MQ发送消息
                                        SendResult result = producer.send(mqMsg);
//                                        log.info(....)
                                        if (result != null && result.getSendStatus() == SendStatus.SEND_OK) {
                                            // 修改数据库的状态
                                            int res = msgStorage.updateMsgStatus(dataSrc, msgInfo.getId());

                                        }
                                    } catch (Exception e) {
                                        log.error("SchedScanMsg deal fail", e);
                                    }
                                }
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                log.error("getWaitMsg fail", e);
                            }
                        }
                    }
                }
            }
        }

    }

}
