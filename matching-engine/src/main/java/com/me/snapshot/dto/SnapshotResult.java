package com.me.snapshot.dto;

/**
 * 一次快照操作的结果摘要。
 *
 * <p>{@code backupId} 是 MySQL {@code snapshot_backup.id}，管理端可用它查询
 * 本次快照的逐分区恢复 offset；{@code fileSizeBytes} 是落盘文件大小。
 */
public record SnapshotResult(
        long backupId,
        String path,
        long fileSizeBytes,
        String createdAt,
        long accountSeq,
        long orderSeq,
        int accountCount,
        int orderCount,
        int bookCount,
        int offsetCount,
        long durationMillis,
        String checksum) {
}
