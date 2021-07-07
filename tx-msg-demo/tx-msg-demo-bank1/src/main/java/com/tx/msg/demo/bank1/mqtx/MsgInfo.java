package com.tx.msg.demo.bank1.mqtx;

import lombok.Data;

/**
 * @Author: WuYunXi
 * @Date: 2021/7/5 0005 22:36
 **/
@Data
public class MsgInfo {
    private String  topic;
    private String tag;
    private String content;
    private String id;
}
