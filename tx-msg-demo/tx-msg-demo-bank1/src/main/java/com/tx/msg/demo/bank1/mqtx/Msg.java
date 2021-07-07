package com.tx.msg.demo.bank1.mqtx;

import lombok.Data;

/**
 * @Author: WuYunXi
 * @Date: 2021/7/5 0005 20:27
 **/
@Data
public class Msg {

    private Long id;
    private String idUrlPair;
    private long createTime;
    private long nextExpireTime;
    private int haveDealedTimes;

    public Msg(Long id, String idUrlPair) {
        this.id = id;
        this.idUrlPair = idUrlPair;

    }
}
