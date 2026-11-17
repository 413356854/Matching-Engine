package com.me.snapshot.dto;

/**
 * 快照切点上某条状态机输入 topic 分区的下一个消费 offset。
 *
 * <p>恢复时必须把消费组重置到该 offset，而不是使用 broker 当前的 committed offset：
 * broker 的 committed offset 可能已经领先于快照。
 */
public record TopicPartitionOffset(
        String topic,
        String group,
        int partition,
        long nextOffset) {
}
