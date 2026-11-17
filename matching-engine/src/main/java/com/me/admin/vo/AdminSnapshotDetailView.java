package com.me.admin.vo;

import com.me.snapshot.dto.SnapshotBackupOffsetRecord;
import com.me.snapshot.dto.SnapshotBackupRecord;
import java.util.List;

/**
 * 管理端快照详情：快照记录 + 六条状态机输入 topic 的逐分区恢复 offset。
 */
public record AdminSnapshotDetailView(
        boolean success,
        String reason,
        SnapshotBackupRecord backup,
        List<SnapshotBackupOffsetRecord> offsets) {

    public AdminSnapshotDetailView {
        offsets = offsets == null ? List.of() : List.copyOf(offsets);
    }

    public static AdminSnapshotDetailView success(SnapshotBackupRecord backup,
                                                  List<SnapshotBackupOffsetRecord> offsets) {
        return new AdminSnapshotDetailView(true, null, backup, offsets);
    }

    public static AdminSnapshotDetailView rejected(String reason) {
        return new AdminSnapshotDetailView(false, reason, null, List.of());
    }
}
