package com.me.account.service;

/**
 * 充值入口：把一条 {@code DEPOSIT} 资金命令投递到账户模块的 Kafka 命令队列。
 *
 * <p>充值异步生效：返回的 {@code requestId} 是账户模块的幂等键，
 * 前端可通过余额查询接口轮询入账结果。
 */
public interface RechargeService {

    /**
     * 提交一笔充值命令。
     *
     * @param userId   用户 ID
     * @param currency 币种代码，统一转大写
     * @param amount   币种最小单位整数
     * @return 本次充值的幂等键
     */
    String recharge(long userId, String currency, long amount);
}
