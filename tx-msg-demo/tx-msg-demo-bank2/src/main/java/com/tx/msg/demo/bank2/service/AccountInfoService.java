package com.tx.msg.demo.bank2.service;

import com.tx.msg.demo.bank2.model.AccountChangeEvent;

/**
 * @Author: ZhuLinHai
 * @Date: 2021/7/5 10:09
 **/
public interface AccountInfoService {

    // 更新账户，增加金额
    public int addAccountInfoBalance(AccountChangeEvent accountChangeEvent);
}
