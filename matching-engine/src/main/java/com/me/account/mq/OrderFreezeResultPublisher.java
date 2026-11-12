package com.me.account.mq;

import com.me.kafka.KafkaSink;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.event.OrderFreezeResultEvent;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把下单冻结回执按 symbol 分组发送到 {@code me.order.freeze-results-batch}：
 * 每个 symbol 一条批量消息，key=symbol；同一 symbol 的回执保持顺序。
 *
 * <p>账户冻结消费者按 symbol 分区并行，如果所有回执都用固定 key {@code batch}，
 * 多个 producer 会争抢同一分区并产生事务冲突。按 symbol 分组后，每个 symbol 的
 * 回执只由处理该 symbol 的 producer 写入对应分区，下游也可以按 symbol 并行消费。
 *
 * <p>发送与「冻结命令消费 offset + 资金流水」处于同一个 Kafka 事务：成功命令不会出现
 * 「余额已冻结、回执丢失」，失败命令也不会出现「offset 已提交、回执丢失」。
 */
@Component
public class OrderFreezeResultPublisher {

    private final KafkaSink sink;
    private final String topic;

    public OrderFreezeResultPublisher(
            KafkaSink sink,
            @Value("${app.kafka.topics.order-freeze-results}") String topic) {
        this.sink = sink;
        this.topic = topic;
    }

    /**
     * 按 symbol 分组发送批量回执；每组一条消息，key=symbol。
     *
     * @param eventsBySymbol symbol → 该 symbol 的回执列表，列表顺序即处理顺序
     */
    public void publishBatch(Map<String, List<OrderFreezeResultEvent>> eventsBySymbol) {
        eventsBySymbol.forEach((symbol, events) ->
                sink.sendBatchMessage(topic, symbol, events, OrderFreezeResultBatch::new));
    }
}
