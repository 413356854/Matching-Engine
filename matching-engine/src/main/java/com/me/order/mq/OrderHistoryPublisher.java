package com.me.order.mq;

import com.me.kafka.KafkaSink;
import com.me.shared.order.OrderHistoryBatch;
import com.me.shared.order.OrderHistoryEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把进入终态的委托（撤单 / 撮合完成）按 symbol 分组写入 {@code me.order.history-batch}：
 * 每个 symbol 一条 {@link OrderHistoryBatch} 批量消息，key=symbol
 * （超过 2000 条时由 {@link KafkaSink} 拆成多条）。
 *
 * <p>发送发生在消费 runner 的 Kafka 事务内：撤单历史与撤单 offset 同事务提交/回滚；
 * 撮合产生的历史与冻结回执 offset、撮合对、结算命令同事务提交/回滚。
 */
@Component
public class OrderHistoryPublisher {

    private final KafkaSink sink;
    private final String topic;

    public OrderHistoryPublisher(KafkaSink sink,
                                 @Value("${app.kafka.topics.order-history}") String topic) {
        this.sink = sink;
        this.topic = topic;
    }

    /** 按 symbol 分组发送委托历史批量消息，由当前 Kafka 事务统一提交。 */
    public void publishBatch(List<OrderHistoryEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        Map<String, List<OrderHistoryEvent>> bySymbol = new LinkedHashMap<>();
        for (OrderHistoryEvent event : events) {
            bySymbol.computeIfAbsent(event.symbol(), ignored -> new ArrayList<>()).add(event);
        }
        bySymbol.forEach((symbol, batch) ->
                sink.sendBatchMessage(topic, symbol, batch, OrderHistoryBatch::new));
    }
}
