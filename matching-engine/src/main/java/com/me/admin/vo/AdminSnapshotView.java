package com.me.admin.vo;

import com.me.snapshot.dto.SnapshotResult;

/**
 * 管理端快照接口的响应。
 */
public record AdminSnapshotView(
        boolean success,
        Long backupId,
        String path,
        Long fileSizeBytes,
        String createdAt,
        Long accountSeq,
        Long orderSeq,
        Integer accountCount,
        Integer orderCount,
        Integer bookCount,
        Integer offsetCount,
        Long durationMillis,
        String checksum,
        String error) {

    public static AdminSnapshotView success(SnapshotResult result) {
        return new AdminSnapshotView(
                true,
                result.backupId(),
                result.path(),
                result.fileSizeBytes(),
                result.createdAt(),
                result.accountSeq(),
                result.orderSeq(),
                result.accountCount(),
                result.orderCount(),
                result.bookCount(),
                result.offsetCount(),
                result.durationMillis(),
                result.checksum(),
                null);
    }

    public static AdminSnapshotView failure(String error) {
        return new AdminSnapshotView(
                false, null, null, null, null, null, null, null, null, null, null, null,
                null, error);
    }
}
