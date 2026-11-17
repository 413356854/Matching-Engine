package com.me.admin.vo;

import com.me.snapshot.dto.SnapshotBackupRecord;
import java.util.List;

/**
 * 管理端快照列表：按 id 倒序，最新快照在前。
 */
public record AdminSnapshotQueryView(
        boolean success,
        String reason,
        List<SnapshotBackupRecord> backups) {

    public AdminSnapshotQueryView {
        backups = backups == null ? List.of() : List.copyOf(backups);
    }

    public static AdminSnapshotQueryView success(List<SnapshotBackupRecord> backups) {
        return new AdminSnapshotQueryView(true, null, backups);
    }

    public static AdminSnapshotQueryView rejected(String reason) {
        return new AdminSnapshotQueryView(false, reason, List.of());
    }
}
