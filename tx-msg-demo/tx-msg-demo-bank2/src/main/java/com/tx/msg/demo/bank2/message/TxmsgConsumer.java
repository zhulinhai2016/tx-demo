package com.tx.msg.demo.bank2.message;

import com.alibaba.fastjson.JSONObject;
import com.tx.msg.demo.bank2.model.AccountChangeEvent;
import com.tx.msg.demo.bank2.service.AccountInfoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 9:15
 **/
@Component
@Slf4j
@RocketMQMessageListener(consumerGroup = "consumer_group_txmsg_bank2",topic = "topic_txmsg")
public class TxmsgConsumer implements RocketMQListener<String> {

    @Autowired
    AccountInfoService accountInfoService;

    @Override
    public void onMessage(String message) {
        // 收到消息后进行消费，对李四的银行账户进行加减金额
        log.info("开始消费消息：{}",message);
        JSONObject jsonObject = JSONObject.parseObject(message);
        String accountChange = jsonObject.getString("accountChange");

        AccountChangeEvent accountChangeEvent = JSONObject.parseObject(accountChange, AccountChangeEvent.class);
        // 设置账号为李四的账号
        accountChangeEvent.setAccountNo("2");
        // 增加金额
        accountInfoService.addAccountInfoBalance(accountChangeEvent);

    }
}
