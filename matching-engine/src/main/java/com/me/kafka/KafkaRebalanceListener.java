package com.me.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分区分配时显式从已提交 offset 继续；没有提交记录时由 {@code auto.offset.reset}
 * （生产默认 {@code earliest}）决定起始位置。
 *
 * <p>替代原 Spring Kafka 的 {@code KafkaOffsetRebalanceListener}。offset 只通过
 * {@code sendOffsetsToTransaction} 随事务提交，因此查到的位置天然只包含已提交事务的进度。
 */
public final class KafkaRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRebalanceListener.class);

    private final Consumer<?, ?> consumer;

    public KafkaRebalanceListener(Consumer<?, ?> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> committed =
                consumer.committed(Set.copyOf(partitions));
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata offset = committed.get(partition);
            // 没有已提交 offset 时不在这里 seekToBeginning：让 auto.offset.reset 决定
            // （生产 earliest，压测 latest）。
            if (offset != null) {
                consumer.seek(partition, offset.offset());
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.warn("partitions lost: {}", partitions);
    }
}
