package com.tx.msg.demo.bank1.service.impl;

import com.tx.msg.demo.bank1.dao.AccountInfoDao;
import com.tx.msg.demo.bank1.model.AccountChangeEvent;
import com.tx.msg.demo.bank1.mqtx.MybatisTransactionMsgClient;
import com.tx.msg.demo.bank1.service.AccountInfoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Administrator
 * @version 1.0
 **/
@Service
@Slf4j
public class AccountInfoServiceImpl implements AccountInfoService {

    @Autowired
    AccountInfoDao accountInfoDao;

    @Autowired
    RocketMQTemplate rocketMQTemplate;
//    @Autowired
//    DbPool dbPool;
    @Autowired
    MybatisTransactionMsgClient msgClient;

    //向mq发送转账消息
    @Override
    @Transactional
    public void sendUpdateAccountBalance(AccountChangeEvent accountChangeEvent) {

        //将accountChangeEvent转成json
//        JSONObject jsonObject =new JSONObject();
//        jsonObject.put("accountChange",accountChangeEvent);
//        String jsonString = jsonObject.toJSONString();
//        //生成message类型
//        Message<String> message = MessageBuilder.withPayload(jsonString).build();
        //发送一条事务消息
        /**
         * String txProducerGroup 生产组
         * String destination topic，
         * Message<?> message, 消息内容
         * Object arg 参数
         */
//        rocketMQTemplate.sendMessageInTransaction("producer_group_txmsg_bank1","topic_txms",message,null);
        //幂等判断
        if(accountInfoDao.isExistTx(accountChangeEvent.getTxNo())>0){
            return ;
        }
        //扣减金额
        accountInfoDao.updateAccountBalance(accountChangeEvent.getAccountNo(),accountChangeEvent.getAmount() * -1);
        //添加事务日志
        accountInfoDao.addTx(accountChangeEvent.getTxNo());
        if(accountChangeEvent.getAmount() == 3){
            throw new RuntimeException("人为制造异常");
        }
        try {
            msgClient.sendMsg("test","txTextTopic","txMsg");
        } catch (Exception e) {
            throw  new RuntimeException();
        }
    }

    //更新账户，扣减金额
    @Override
    @Transactional
    public void doUpdateAccountBalance(AccountChangeEvent accountChangeEvent) {
        //幂等判断
        if(accountInfoDao.isExistTx(accountChangeEvent.getTxNo())>0){
            return ;
        }
        //扣减金额
        accountInfoDao.updateAccountBalance(accountChangeEvent.getAccountNo(),accountChangeEvent.getAmount() * -1);
        //添加事务日志
        accountInfoDao.addTx(accountChangeEvent.getTxNo());
        if(accountChangeEvent.getAmount() == 3){
            throw new RuntimeException("人为制造异常");
        }
    }
}
