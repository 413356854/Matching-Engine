package com.me.snapshot.recovery;

import com.me.snapshot.SnapshotRecoveryException;
import com.me.snapshot.dto.TopicPartitionOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据快照 offset、broker committed offset 和 offset-seq 映射，推导恢复后的消费起点。
 *
 * <p>规则：
 * <ul>
 *   <li>broker offset 缺失或小于快照 offset：快照状态已经包含这些记录，必须把组位点
 *       写回快照 offset，不能重放，否则会重复应用；</li>
 *   <li>broker offset 等于快照 offset：无尾部；</li>
 *   <li>broker offset 大于快照 offset：重放 [快照 offset, broker offset)，完成后仍然从
 *       broker offset 继续。该区间必须被 offset-seq 映射完整覆盖。</li>
 * </ul>
 */
public final class RecoveryOffsetPlanner {

    private RecoveryOffsetPlanner() {
    }

    /**
     * 为每个输入分区推导「从哪里开始重放、恢复后消费位点停在哪」。
     *
     * <p><b>符号</b>：S = 快照记录的 offset（快照切点后的下一条记录），C = broker 上
     * 该消费组当前的 committed offset（最后已提交记录 + 1）。
     *
     * <p><b>分支规则</b>：
     * <ol>
     *   <li><b>C 缺失</b>：组从未提交，快照状态不包含任何未提交输入。恢复后必须从 S
     *       开始消费（Action=SET），否则 earliest 会把 S 之前的记录重新执行；</li>
     *   <li><b>C &lt; S</b>：组位点退回到快照之前。快照已经包含 [C, S) 的处理结果，
     *       必须把位点抬到 S（Action=SET），不能重放这段；</li>
     *   <li><b>C == S</b>：没有尾部（Action=KEEP）；</li>
     *   <li><b>C &gt; S</b>：快照之后有已提交输入，重放 [S, C)，消费位点保持 C
     *       （Action=KEEP）。该区间必须被 offset-seq 映射完整覆盖：最大映射 offset
     *       必须等于 C-1，否则说明有已提交批次没有映射，状态会漏。</li>
     * </ol>
     *
     * <p><b>映射校验</b>：所有映射必须落在 [S, C) 内，且其所在分区不能是 Action=SET
     * （SET 表示该分区没有可重放的已提交记录）。任何越界 / 缺失 / 冲突都直接抛
     * {@link SnapshotRecoveryException}，不尝试“尽量恢复”。
     *
     * @param brokerOffsets    {group, topic, partition} → broker committed offset；缺失表示组没有提交记录
     * @param mappedCounts     每个输入分区的重放映射条数
     * @param maxMappedOffsets 每个输入分区的最大映射 offset
     * @param mappings         全部重放映射，用于校验都落在重放区间内
     */
    public static RecoveryOffsetPlan plan(
            List<TopicPartitionOffset> inputOffsets,
            Map<OffsetKey, Long> brokerOffsets,
            Map<OffsetKey, Long> mappedCounts,
            Map<OffsetKey, Long> maxMappedOffsets,
            List<ReplayMapping> mappings) {
        if (inputOffsets == null || inputOffsets.isEmpty()) {
            throw new SnapshotRecoveryException("snapshot has no input offsets");
        }

        Map<String, String> groupByTopic = new HashMap<>();
        Map<OffsetKey, TopicPartitionOffset> byKey = new HashMap<>();
        for (TopicPartitionOffset offset : inputOffsets) {
            String previous = groupByTopic.putIfAbsent(offset.topic(), offset.group());
            if (previous != null && !previous.equals(offset.group())) {
                throw new SnapshotRecoveryException("input topic " + offset.topic()
                        + " is recorded with multiple consumer groups: "
                        + previous + ", " + offset.group());
            }
            OffsetKey key = new OffsetKey(offset.group(), offset.topic(), offset.partition());
            if (byKey.putIfAbsent(key, offset) != null) {
                throw new SnapshotRecoveryException("duplicate snapshot input offset: " + key);
            }
        }

        List<RecoveryOffsetPlan.PartitionPlan> plans = new ArrayList<>(inputOffsets.size());
        Map<OffsetKey, RecoveryOffsetPlan.PartitionPlan> planByKey = new LinkedHashMap<>();
        for (TopicPartitionOffset offset : inputOffsets) {
            OffsetKey key = new OffsetKey(offset.group(), offset.topic(), offset.partition());
            long snapshotOffset = offset.nextOffset();
            Long brokerOffset = brokerOffsets.get(key);
            long mappedCount = mappedCounts.getOrDefault(key, 0L);
            Long maxMapped = maxMappedOffsets.get(key);

            RecoveryOffsetPlan.Action action;
            long resumeOffset;
            if (brokerOffset == null || brokerOffset < snapshotOffset) {
                if (mappedCount > 0) {
                    throw new SnapshotRecoveryException("partition " + key
                            + " has replay mappings but broker offset " + brokerOffset
                            + " is below snapshot offset " + snapshotOffset);
                }
                action = RecoveryOffsetPlan.Action.SET;
                resumeOffset = snapshotOffset;
            } else if (brokerOffset == snapshotOffset) {
                if (mappedCount > 0) {
                    throw new SnapshotRecoveryException("partition " + key
                            + " has replay mappings but broker offset equals snapshot offset "
                            + snapshotOffset);
                }
                action = RecoveryOffsetPlan.Action.KEEP;
                resumeOffset = snapshotOffset;
            } else {
                long expectedLast = brokerOffset - 1;
                if (mappedCount == 0) {
                    throw new SnapshotRecoveryException("partition " + key
                            + " has committed records after the snapshot offset "
                            + snapshotOffset + " but no offset-seq mappings");
                }
                if (maxMapped == null || maxMapped != expectedLast) {
                    throw new SnapshotRecoveryException("partition " + key
                            + " replay mappings end at " + maxMapped
                            + " but broker committed offset is " + brokerOffset
                            + " (expected last mapped offset " + expectedLast
                            + "); input or offset-seq records may have been deleted");
                }
                action = RecoveryOffsetPlan.Action.KEEP;
                resumeOffset = brokerOffset;
            }

            RecoveryOffsetPlan.PartitionPlan plan = new RecoveryOffsetPlan.PartitionPlan(
                    offset.topic(), offset.group(), offset.partition(), snapshotOffset,
                    brokerOffset, resumeOffset, action);
            plans.add(plan);
            planByKey.put(key, plan);
        }

        for (ReplayMapping mapping : mappings) {
            String group = groupByTopic.get(mapping.topic());
            if (group == null) {
                throw new SnapshotRecoveryException("replay mapping references unknown input "
                        + "topic " + mapping.topic());
            }
            OffsetKey key = new OffsetKey(group, mapping.topic(), mapping.partition());
            RecoveryOffsetPlan.PartitionPlan plan = planByKey.get(key);
            if (plan == null) {
                throw new SnapshotRecoveryException("replay mapping references unknown input "
                        + "partition " + key);
            }
            if (plan.action() == RecoveryOffsetPlan.Action.SET) {
                throw new SnapshotRecoveryException("replay mapping exists for partition "
                        + key + " but its broker offset is below the snapshot offset");
            }
            if (mapping.offset() < plan.snapshotOffset()
                    || mapping.offset() >= plan.resumeOffset()) {
                throw new SnapshotRecoveryException("replay mapping offset "
                        + mapping.offset() + " on " + key + " is outside the replay window ["
                        + plan.snapshotOffset() + ", " + plan.resumeOffset() + ")");
            }
        }

        plans.sort(Comparator.comparing(RecoveryOffsetPlan.PartitionPlan::topic)
                .thenComparing(RecoveryOffsetPlan.PartitionPlan::group)
                .thenComparingInt(RecoveryOffsetPlan.PartitionPlan::partition));
        return new RecoveryOffsetPlan(plans);
    }
}
