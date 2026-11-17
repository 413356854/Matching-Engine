package com.me.snapshot;

/**
 * 已有一个快照在执行；同一进程内只允许单飞。
 */
public class SnapshotInProgressException extends RuntimeException {

    public SnapshotInProgressException(String message) {
        super(message);
    }
}
