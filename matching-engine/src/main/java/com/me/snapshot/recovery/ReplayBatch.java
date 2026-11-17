package com.me.snapshot.recovery;

import java.util.List;

/**
 * 一个原始消费批次对应的重放单元。
 *
 * <p>一条 offset-seq Kafka 消息就是原模块的一次 service 调用；保留批次边界可以让重放
 * 复用相同的批内并发 / 聚合语义。{@code mappings} 按原始输入顺序排列，seq 连续。
 */
public record ReplayBatch(String topic, List<ReplayMapping> mappings) {

    public ReplayBatch {
        mappings = List.copyOf(mappings);
        if (mappings.isEmpty()) {
            throw new IllegalArgumentException("replay batch mappings must not be empty");
        }
    }

    public long firstSeq() {
        return mappings.get(0).seq();
    }

    public long lastSeq() {
        return mappings.get(mappings.size() - 1).seq();
    }
}
