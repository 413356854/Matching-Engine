package com.me.shared.event;

import java.util.List;

/**
 * 成交行情批量消息：委托模块撮合完一个消费批次后，把该批产生的全部
 * {@link MarketDataEvent} 封装成一条 Kafka 消息，发往 {@code me.marketdata-batch}。
 *
 * <p>批内顺序即撮合产生成交的顺序，行情模块按列表顺序聚合 K 线。
 */
public class MarketDataBatch {

    private List<MarketDataEvent> msgList = List.of();

    public MarketDataBatch() {
    }

    public MarketDataBatch(List<MarketDataEvent> msgList) {
        setMsgList(msgList);
    }

    public List<MarketDataEvent> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<MarketDataEvent> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
