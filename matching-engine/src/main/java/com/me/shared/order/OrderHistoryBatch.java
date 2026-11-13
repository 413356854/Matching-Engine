package com.me.shared.order;

import java.util.List;

/**
 * 委托历史批量消息，{@code me.order.history-batch} 的跨模块契约。
 *
 * <p>委托模块把同一个消费批次内进入终态（{@link OrderStatus#CANCELED} /
 * {@link OrderStatus#FILLED}）的委托按 symbol 分组，每个 symbol 封装成一条
 * {@link OrderHistoryBatch}，以 {@code key=symbol} 发往 Kafka。admin 模块消费后
 * 按列表顺序归档到 QuestDB {@code ord_history}，并按 orderId 去重。
 *
 * <p>批内顺序即委托进入终态的顺序；同一 symbol 的消息落在同一分区并保持顺序。
 */
public class OrderHistoryBatch {

    private List<OrderHistoryEvent> msgList = List.of();

    public OrderHistoryBatch() {
    }

    public OrderHistoryBatch(List<OrderHistoryEvent> msgList) {
        setMsgList(msgList);
    }

    public List<OrderHistoryEvent> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<OrderHistoryEvent> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
