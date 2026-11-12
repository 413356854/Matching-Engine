package com.me.shared.event;

import java.util.List;

/**
 * 冻结回执批量消息：账户模块处理完一个消费批次后，把该批产生的全部
 * {@link OrderFreezeResultEvent} 封装成一条 Kafka 消息，发往
 * {@code me.order.freeze-results-batch}，替代逐条发送。
 *
 * <p>批内顺序与账户命令的消费顺序一致，消费端按列表顺序处理。
 */
public class OrderFreezeResultBatch {

    private List<OrderFreezeResultEvent> msgList = List.of();

    public OrderFreezeResultBatch() {
    }

    public OrderFreezeResultBatch(List<OrderFreezeResultEvent> msgList) {
        setMsgList(msgList);
    }

    public List<OrderFreezeResultEvent> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<OrderFreezeResultEvent> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
