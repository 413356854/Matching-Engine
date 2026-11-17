package com.me.snapshot.recovery;

import java.util.List;

/**
 * 每个输入分区在恢复后的消费起点，以及需要写回 broker 的调整项。
 *
 * @param partitions 全部分区的计划，按 topic / group / partition 排序
 */
public record RecoveryOffsetPlan(List<PartitionPlan> partitions) {

    public RecoveryOffsetPlan {
        partitions = List.copyOf(partitions);
    }

    /** 需要调用 {@code alterConsumerGroupOffsets} 的分区。 */
    public List<PartitionPlan> adjustments() {
        return partitions.stream().filter(plan -> plan.action() == Action.SET).toList();
    }

    public enum Action {
        /** broker committed offset 正确，重放后直接从该位点继续。 */
        KEEP,
        /** broker 位点缺失或落后于快照，写回快照位点。 */
        SET
    }

    public record PartitionPlan(
            String topic,
            String group,
            int partition,
            long snapshotOffset,
            Long brokerOffset,
            long resumeOffset,
            Action action) {
    }
}
