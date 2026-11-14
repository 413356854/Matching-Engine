package com.me.order.mq;

import com.me.kafka.KafkaSink;
import com.me.shared.event.MatchPairBatch;
import com.me.shared.event.MatchPairEvent;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把进程内撮合产生的撮合对按 symbol 分组写入 {@code me.match-pair-batch}：
 * 每个 symbol 一条 {@link MatchPairBatch} 批量消息，key=symbol
 * （超过 2000 条时由 {@link KafkaSink} 拆成多条）。同一 symbol 的成交保持撮合顺序。
 *
 * <p>发送发生在冻结回执消费 runner 的事务内：与冻结回执 offset、双边结算命令同事务提交/回滚。
 */
@Component
public class MatchPairEventPublisher {

    private final KafkaSink sink;
    private final String topic;

    public MatchPairEventPublisher(KafkaSink sink,
                                   @Value("${app.kafka.topics.match-pair}") String topic) {
        this.sink = sink;
        this.topic = topic;
    }

    /** 按 symbol 分组发送撮合对批量消息，由当前 Kafka 事务统一提交。 */
    public void publishBatch(Map<String, List<MatchPairEvent>> eventsBySymbol) {
        eventsBySymbol.forEach((symbol, events) ->
                sink.sendBatchMessage(topic, symbol, events, MatchPairBatch::new));
    }
}
