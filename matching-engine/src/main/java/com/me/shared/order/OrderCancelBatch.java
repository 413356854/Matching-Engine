package com.me.shared.order;

import java.util.List;

/**
 * 撤单批量消息，{@code me.order.cancels-batch} 的跨模块契约。
 *
 * <p>撤单入口 Disruptor 按 symbol 攒批：任一 symbol 的待发送列表达到 1000 条、或 20ms
 * 定时窗口到达时，把当前 {@code Map<symbol, List<OrderCancelCommand>>} 中的每个 symbol
 * 分别封装成一条 {@link OrderCancelBatch}，以 {@code key=symbol} 发往 Kafka。
 *
 * <p>批内列表顺序即撤单到达顺序；同一 symbol 的批次按聚合线程发送顺序进入同一分区。
 */
public class OrderCancelBatch {

    private List<OrderCancelCommand> msgList = List.of();

    public OrderCancelBatch() {
    }

    public OrderCancelBatch(List<OrderCancelCommand> msgList) {
        setMsgList(msgList);
    }

    public List<OrderCancelCommand> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<OrderCancelCommand> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
