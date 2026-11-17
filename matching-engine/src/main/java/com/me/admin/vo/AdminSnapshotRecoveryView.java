package com.me.admin.vo;

import com.me.snapshot.recovery.RecoveryStatus;

/**
 * 管理端启动恢复状态。
 */
public record AdminSnapshotRecoveryView(
        boolean success,
        String reason,
        RecoveryStatus recovery) {

    public static AdminSnapshotRecoveryView success(RecoveryStatus recovery) {
        return new AdminSnapshotRecoveryView(true, null, recovery);
    }

    public static AdminSnapshotRecoveryView rejected(String reason) {
        return new AdminSnapshotRecoveryView(false, reason, null);
    }
}
