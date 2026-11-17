package com.me.snapshot;

/**
 * 快照加载 / 重放失败：文件损坏、格式不兼容、offset 或 seq 不一致、Kafka 记录缺失等。
 *
 * <p>恢复是启动流程的一部分，任何失败都会阻止 consumer 启动，避免带着部分状态运行。
 */
public class SnapshotRecoveryException extends RuntimeException {

    public SnapshotRecoveryException(String message) {
        super(message);
    }

    public SnapshotRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
