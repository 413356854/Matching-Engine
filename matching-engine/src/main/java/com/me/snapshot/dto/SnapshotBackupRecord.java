package com.me.snapshot.dto;

/**
 * 管理端展示用的一条快照记录（不含逐分区 offset）。
 */
public record SnapshotBackupRecord(
        long id,
        int formatVersion,
        String instanceId,
        String filePath,
        long fileSizeBytes,
        String checksum,
        String snapshotCreatedAt,
        long accountSeq,
        long orderSeq,
        int accountCount,
        int orderCount,
        int bookCount,
        int offsetCount,
        long durationMs,
        String createdAt) {
}
