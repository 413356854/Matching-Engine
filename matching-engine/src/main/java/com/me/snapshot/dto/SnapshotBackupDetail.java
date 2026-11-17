package com.me.snapshot.dto;

import java.util.List;

/**
 * 快照详情：记录 + 六条状态机输入 topic 的逐分区恢复 offset。
 */
public record SnapshotBackupDetail(
        SnapshotBackupRecord backup,
        List<SnapshotBackupOffsetRecord> offsets) {

    public SnapshotBackupDetail {
        offsets = offsets == null ? List.of() : List.copyOf(offsets);
    }
}
