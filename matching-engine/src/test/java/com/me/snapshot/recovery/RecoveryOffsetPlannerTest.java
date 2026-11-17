package com.me.snapshot.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.me.snapshot.SnapshotRecoveryException;
import com.me.snapshot.dto.TopicPartitionOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RecoveryOffsetPlannerTest {

    private static final String TOPIC = "input";
    private static final String GROUP = "group";

    @Test
    void keepsBrokerOffsetWhenTailMappingsCoverWindow() {
        RecoveryOffsetPlan plan = RecoveryOffsetPlanner.plan(
                offsets(5),
                Map.of(key(), 10L),
                Map.of(key(), 5L),
                Map.of(key(), 9L),
                mappings(5, 9));

        assertEquals(1, plan.partitions().size());
        RecoveryOffsetPlan.PartitionPlan partition = plan.partitions().get(0);
        assertEquals(RecoveryOffsetPlan.Action.KEEP, partition.action());
        assertEquals(10L, partition.resumeOffset());
        assertTrue(plan.adjustments().isEmpty());
    }

    @Test
    void setsSnapshotOffsetWhenBrokerOffsetIsMissing() {
        RecoveryOffsetPlan plan = RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(), Map.of(), Map.of(), List.of());

        RecoveryOffsetPlan.PartitionPlan partition = plan.partitions().get(0);
        assertEquals(RecoveryOffsetPlan.Action.SET, partition.action());
        assertEquals(5L, partition.resumeOffset());
        assertEquals(1, plan.adjustments().size());
    }

    @Test
    void setsSnapshotOffsetWhenBrokerOffsetIsBehind() {
        RecoveryOffsetPlan plan = RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 3L), Map.of(), Map.of(), List.of());

        RecoveryOffsetPlan.PartitionPlan partition = plan.partitions().get(0);
        assertEquals(RecoveryOffsetPlan.Action.SET, partition.action());
        assertEquals(5L, partition.resumeOffset());
    }

    @Test
    void keepsWhenBrokerOffsetEqualsSnapshot() {
        RecoveryOffsetPlan plan = RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 5L), Map.of(), Map.of(), List.of());

        RecoveryOffsetPlan.PartitionPlan partition = plan.partitions().get(0);
        assertEquals(RecoveryOffsetPlan.Action.KEEP, partition.action());
        assertEquals(5L, partition.resumeOffset());
        assertTrue(plan.adjustments().isEmpty());
    }

    @Test
    void rejectsTailWithoutMappings() {
        assertThrows(SnapshotRecoveryException.class, () -> RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 10L), Map.of(), Map.of(), List.of()));
    }

    @Test
    void rejectsIncompleteMappings() {
        assertThrows(SnapshotRecoveryException.class, () -> RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 10L),
                Map.of(key(), 4L), Map.of(key(), 8L), mappings(5, 8)));
    }

    @Test
    void rejectsMappingBeforeSnapshotWindow() {
        assertThrows(SnapshotRecoveryException.class, () -> RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 10L),
                Map.of(key(), 5L), Map.of(key(), 9L), mappings(4, 8)));
    }

    @Test
    void rejectsMappingAtBrokerOffset() {
        assertThrows(SnapshotRecoveryException.class, () -> RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 10L),
                Map.of(key(), 5L), Map.of(key(), 9L), mappings(6, 10)));
    }

    @Test
    void rejectsMappingForPartitionWithBrokerBehind() {
        assertThrows(SnapshotRecoveryException.class, () -> RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 3L),
                Map.of(key(), 1L), Map.of(key(), 4L),
                List.of(new ReplayMapping(TOPIC, 0, 4L, 100L))));
    }

    @Test
    void rejectsMappingForUnknownTopic() {
        assertThrows(SnapshotRecoveryException.class, () -> RecoveryOffsetPlanner.plan(
                offsets(5), Map.of(key(), 5L), Map.of(), Map.of(),
                List.of(new ReplayMapping("other", 0, 5L, 100L))));
    }

    private static List<TopicPartitionOffset> offsets(long nextOffset) {
        return List.of(new TopicPartitionOffset(TOPIC, GROUP, 0, nextOffset));
    }

    private static OffsetKey key() {
        return new OffsetKey(GROUP, TOPIC, 0);
    }

    private static List<ReplayMapping> mappings(long from, long to) {
        List<ReplayMapping> mappings = new ArrayList<>();
        long seq = 100L;
        for (long offset = from; offset <= to; offset++) {
            mappings.add(new ReplayMapping(TOPIC, 0, offset, seq++));
        }
        return mappings;
    }
}
