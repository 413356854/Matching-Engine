package com.me.snapshot.dto;

/**
 * 管理端展示用的一条快照恢复 offset。
 */
public record SnapshotBackupOffsetRecord(
        String topic,
        String group,
        int partition,
        long nextOffset) {
}
