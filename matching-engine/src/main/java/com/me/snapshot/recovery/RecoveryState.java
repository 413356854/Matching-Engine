package com.me.snapshot.recovery;

import com.me.kafka.KafkaClientProperties;
import com.me.snapshot.SnapshotRecoveryProperties;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

/**
 * 启动恢复的全局状态。
 *
 * <p>HTTP 过滤器用它决定是否放行请求：恢复未完成时返回 503，避免外部读到恢复中的
 * 半状态；恢复未启用时直接视为 ready。
 */
@Component
public class RecoveryState {

    public enum Phase {
        DISABLED,
        PENDING,
        RUNNING,
        READY,
        FAILED
    }

    private final AtomicReference<Phase> phase;
    private final AtomicReference<RecoveryStatus> status;

    public RecoveryState(SnapshotRecoveryProperties properties,
                         KafkaClientProperties kafkaProperties) {
        boolean enabled = kafkaProperties.enabled() && properties.enabled();
        Phase initialPhase = enabled ? Phase.PENDING : Phase.DISABLED;
        this.phase = new AtomicReference<>(initialPhase);
        this.status = new AtomicReference<>(RecoveryStatus.initial(
                initialPhase.name(), !enabled,
                enabled ? "waiting for snapshot recovery" : "snapshot recovery is disabled"));
    }

    /** 恢复未启用或已经完成时放行 HTTP；PENDING / RUNNING / FAILED 一律拒绝。 */
    public boolean isReady() {
        Phase current = phase.get();
        return current == Phase.DISABLED || current == Phase.READY;
    }

    public Phase phase() {
        return phase.get();
    }

    public RecoveryStatus status() {
        return status.get();
    }

    public void running(String message) {
        phase.set(Phase.RUNNING);
        status.updateAndGet(current -> new RecoveryStatus(
                Phase.RUNNING.name(), false, current.backupId(), current.filePath(),
                current.snapshotCreatedAt(), current.accountSeq(), current.orderSeq(),
                current.accountCount(), current.orderCount(), current.bookCount(),
                current.replayedOrderBatches(), current.replayedOrderRecords(),
                current.replayedAccountBatches(), current.replayedAccountRecords(),
                current.adjustedGroups(), current.startedAt(), null, message));
    }

    public void ready(RecoveryStatus completed) {
        status.set(new RecoveryStatus(
                Phase.READY.name(), true, completed.backupId(), completed.filePath(),
                completed.snapshotCreatedAt(), completed.accountSeq(), completed.orderSeq(),
                completed.accountCount(), completed.orderCount(), completed.bookCount(),
                completed.replayedOrderBatches(), completed.replayedOrderRecords(),
                completed.replayedAccountBatches(), completed.replayedAccountRecords(),
                completed.adjustedGroups(), completed.startedAt(),
                completed.finishedAt(), completed.message()));
        phase.set(Phase.READY);
    }

    public void failed(String message) {
        status.updateAndGet(current -> new RecoveryStatus(
                Phase.FAILED.name(), false, current.backupId(), current.filePath(),
                current.snapshotCreatedAt(), current.accountSeq(), current.orderSeq(),
                current.accountCount(), current.orderCount(), current.bookCount(),
                current.replayedOrderBatches(), current.replayedOrderRecords(),
                current.replayedAccountBatches(), current.replayedAccountRecords(),
                current.adjustedGroups(), current.startedAt(),
                java.time.Instant.now().toString(), message));
        phase.set(Phase.FAILED);
    }
}
