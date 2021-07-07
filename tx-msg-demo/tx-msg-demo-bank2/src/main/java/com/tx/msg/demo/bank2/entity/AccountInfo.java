package com.tx.msg.demo.bank2.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 10:06
 **/
@Data
public class AccountInfo implements Serializable {
    private Long id;
    private String accountName;
    private String accountNo;
    private String accountPassword;
    private Double accountBalance;
}
