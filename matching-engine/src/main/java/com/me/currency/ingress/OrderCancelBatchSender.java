package com.me.currency.ingress;

import com.me.shared.order.OrderCancelCommand;
import java.util.List;

/**
 * 聚合后的撤单批量消息发送出口。
 *
 * <p>一次 flush 可能包含多个 symbol，按 symbol 分组后每组调用一次 {@link #send}：
 * Kafka 消息只能有一个 key，只有保持 {@code key=symbol} 才能让同一交易对的撤单
 * 始终落在同一分区、按序处理。
 */
public interface OrderCancelBatchSender {

    /**
     * 异步发送一条 {@code OrderCancelBatch}。
     *
     * @param symbol  交易对，作为 Kafka key
     * @param cancels 该 symbol 的撤单命令，顺序即到达顺序
     */
    void send(String symbol, List<OrderCancelCommand> cancels);
}
