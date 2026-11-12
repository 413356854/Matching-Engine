package com.me.account.mq;

import com.me.kafka.KafkaSink;
import com.me.shared.event.FundsFlowBatch;
import com.me.shared.event.FundsFlowEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把资金流水按 userId 分区发送到 {@code me.funds-batch}。
 *
 * <p>同一批流水先按 userId 的 murmur2 哈希落到目标分区，每个分区聚合成一条
 * {@link FundsFlowBatch} 批量消息（≤ 分区数条），而不是每个用户一条。这样：
 * <ul>
 *   <li>同一用户的流水始终落在同一分区、保持顺序；</li>
 *   <li>不同用户分布到多个分区，funds-flow 消费组可以起多个 consumer 并行归档 QuestDB；</li>
 *   <li>一个账户/结算批次最多产生「分区数」条 funds 消息，避免消息数被用户数放大。</li>
 * </ul>
 *
 * <p>key 使用该分区内第一条流水的 userId；Kafka 默认分区器对 key 做 murmur2，落点与这里
 * 计算的分区一致。发送发生在当前消费 runner 绑定的 Kafka 事务 producer 上；失败由
 * {@code commitTransaction()} 统一暴露，整批回滚并重投。
 */
@Component
public class FundsFlowPublisher {

    private final KafkaSink sink;
    private final String topic;
    private final int partitions;

    public FundsFlowPublisher(
            KafkaSink sink,
            @Value("${app.kafka.topics.funds-flow}") String topic,
            @Value("${app.kafka.topic.partitions:8}") int partitions) {
        this.sink = sink;
        this.topic = topic;
        this.partitions = Math.max(1, partitions);
    }

    /** 按 userId 目标分区聚合，每个分区一条批量消息入队，由当前 Kafka 事务统一提交。 */
    public void publishBatch(List<FundsFlowEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        Map<Integer, PartitionBatch> byPartition = new LinkedHashMap<>();
        for (FundsFlowEvent event : events) {
            String userKey = String.valueOf(event.userId());
            int partition = Utils.toPositive(
                    Utils.murmur2(userKey.getBytes(StandardCharsets.UTF_8))) % partitions;
            byPartition.computeIfAbsent(partition,
                    ignored -> new PartitionBatch(userKey, new ArrayList<>()))
                    .events().add(event);
        }
        byPartition.values().forEach(batch ->
                sink.sendBatchMessage(topic, batch.key(), batch.events(), FundsFlowBatch::new));
    }

    private record PartitionBatch(String key, List<FundsFlowEvent> events) {
    }
}
