package com.me.snapshot.recovery;

import java.time.Instant;

/**
 * 管理端可见的启动恢复状态快照。
 *
 * <p>恢复发生在 Spring SmartLifecycle 启动阶段，Web 端口此时已经打开，因此暴露一个
 * 只读状态接口给运维判断“503 是在恢复中还是恢复失败”。
 */
public record RecoveryStatus(
        String phase,
        boolean ready,
        Long backupId,
        String filePath,
        String snapshotCreatedAt,
        Long accountSeq,
        Long orderSeq,
        Integer accountCount,
        Integer orderCount,
        Integer bookCount,
        long replayedOrderBatches,
        long replayedOrderRecords,
        long replayedAccountBatches,
        long replayedAccountRecords,
        int adjustedGroups,
        String startedAt,
        String finishedAt,
        String message) {

    public static RecoveryStatus initial(String phase, boolean ready, String message) {
        return new RecoveryStatus(phase, ready, null, null, null, null, null,
                null, null, null, 0L, 0L, 0L, 0L, 0, null,
                Instant.now().toString(), message);
    }

    public RecoveryStatus withMessage(String nextMessage) {
        return new RecoveryStatus(phase, ready, backupId, filePath, snapshotCreatedAt,
                accountSeq, orderSeq, accountCount, orderCount, bookCount,
                replayedOrderBatches, replayedOrderRecords,
                replayedAccountBatches, replayedAccountRecords,
                adjustedGroups, startedAt, Instant.now().toString(), nextMessage);
    }
}
