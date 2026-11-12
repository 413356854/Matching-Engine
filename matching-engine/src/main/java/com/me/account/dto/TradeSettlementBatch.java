package com.me.account.dto;

import java.util.List;

/**
 * 成交结算批量消息：委托模块撮合完一个消费批次后，把该批产生的、属于同一个
 * userId slot 的 {@link TradeSettlementCommand} 封装成一条 Kafka 消息，发往
 * {@code me.account.settlements-batch}。
 *
 * <p>一笔成交拆成买方、卖方两条单用户命令，因此每条命令只落在一个 slot；
 * 批内顺序即该 slot 的清算顺序，账户模块按列表顺序逐条结算。
 */
public class TradeSettlementBatch {

    private List<TradeSettlementCommand> msgList = List.of();

    public TradeSettlementBatch() {
    }

    public TradeSettlementBatch(List<TradeSettlementCommand> msgList) {
        setMsgList(msgList);
    }

    public List<TradeSettlementCommand> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<TradeSettlementCommand> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
