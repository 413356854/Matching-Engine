package com.me.snapshot.recovery;

/**
 * 状态机输入 topic 的一个分区，带消费组维度。
 */
public record OffsetKey(String group, String topic, int partition) {
}
