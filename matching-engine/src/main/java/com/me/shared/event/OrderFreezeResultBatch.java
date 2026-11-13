package com.me.shared.event;

import java.util.List;

/**
 * 冻结回执批量消息：账户模块处理完一个消费批次后，把该批产生的
 * {@link OrderFreezeResultEvent} 按 symbol 分组，每个 symbol 封装成一条 Kafka 消息，
 * 发往 {@code me.order.freeze-results-batch}（{@code key=symbol}），替代逐条发送。
 *
 * <p>同一 symbol 的批内顺序与账户命令的消费顺序一致，消费端按列表顺序处理；
 * 不同 symbol 的批次落在不同分区，可以由多个 consumer 并行消费。
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
