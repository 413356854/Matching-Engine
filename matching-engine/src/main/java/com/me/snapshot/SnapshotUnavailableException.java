package com.me.snapshot;

/**
 * 当前状态不满足快照条件：fatal、committer 停止、锁 / 排空超时、状态一致性校验失败等。
 */
public class SnapshotUnavailableException extends RuntimeException {

    public SnapshotUnavailableException(String message) {
        super(message);
    }

    public SnapshotUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
