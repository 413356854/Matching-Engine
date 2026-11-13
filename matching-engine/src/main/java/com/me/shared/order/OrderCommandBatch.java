package com.me.shared.order;

import java.util.List;

/**
 * 下单委托命令批量消息，{@code me.order.commands-batch} 的跨模块契约。
 *
 * <p>HTTP 下单先进入进程内 Disruptor，由唯一的聚合线程按 symbol 放入
 * {@code Map<String, List>}；任一 symbol 达到 2000 条或 50ms 窗口到达时，把 map 中所有
 * symbol 的订单分别聚成一批写入 Kafka：同一 symbol 的批次使用 {@code key=symbol}，仍落在
 * 同一分区、按命令顺序处理；不同 symbol 可以按分区并行消费。
 *
 * <p>批内列表顺序即下单顺序，委托模块按列表顺序逐条保存委托并生成冻结命令。
 */
public class OrderCommandBatch {

    private List<OrderCommand> msgList = List.of();

    public OrderCommandBatch() {
    }

    public OrderCommandBatch(List<OrderCommand> msgList) {
        setMsgList(msgList);
    }

    public List<OrderCommand> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<OrderCommand> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
