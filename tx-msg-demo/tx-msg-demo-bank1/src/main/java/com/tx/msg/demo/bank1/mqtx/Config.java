package com.tx.msg.demo.bank1.mqtx;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 14:43
 **/
@Data
@Component
@ConfigurationProperties(prefix = "txmsg")
public class Config {
    public int sendMsgTimeOut;
    public String[] etcdHosts;
    public int deleteTimePeriod;
    public int schedScanTimePeriod;
    public int threadNum;
    public int schedThreadNum;
    public int deleteMsgOneTimeNum;
    public int historyMsgStoreTime;
    public int statsTimePeriod;
}
