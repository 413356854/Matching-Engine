package com.me.account.dto;

import java.util.List;

/**
 * 委托冻结命令批量消息：委托模块处理完一个下单消费批次后，把该批产生的、属于同一个
 * symbol 的 {@link FundsCommand}（{@code logType=ORDER_FREEZE}）封装成一条 Kafka 消息，
 * 发往 {@code me.account.order-freeze-commands-batch}，{@code key=symbol}。
 *
 * <p>批内顺序即该 symbol 的下单顺序，账户模块按列表顺序处理；消费端再按
 * {@code slot(userId)} 拆成 8 组并发冻结，同一用户的冻结、清算、充值共用同一把账户锁。
 * 冻结命令携带 symbol，账户模块按 symbol 汇总冻结回执，保证撮合阶段按 symbol 处理。
 */
public class OrderFreezeCommandsBatch {

    private List<FundsCommand> msgList = List.of();

    public OrderFreezeCommandsBatch() {
    }

    public OrderFreezeCommandsBatch(List<FundsCommand> msgList) {
        setMsgList(msgList);
    }

    public List<FundsCommand> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<FundsCommand> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
