package com.me.snapshot;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 备份快照配置。
 */
@ConfigurationProperties("app.snapshot")
public record SnapshotProperties(
        /** 快照输出目录；相对路径按进程工作目录解析。 */
        @DefaultValue("./snapshots") String directory,
        /** 等待入口锁后，内部链路排空 + committer flush 的最长时间。 */
        @DefaultValue("120000") long drainTimeoutMs,
        /** 获取三个入口锁的最长等待时间。 */
        @DefaultValue("30000") long entryLockTimeoutMs,
        /** 排空后获取账户 / 委托模块锁的最长等待时间。 */
        @DefaultValue("30000") long moduleLockTimeoutMs,
        /** 写文件前校验「委托剩余冻结 == 账户冻结」。 */
        @DefaultValue("true") boolean validateInvariant,
        /** 是否输出带缩进的 JSON；默认紧凑格式。 */
        @DefaultValue("false") boolean prettyPrint) {

    public SnapshotProperties {
        directory = directory == null || directory.isBlank() ? "./snapshots" : directory.strip();
        drainTimeoutMs = Math.max(1L, drainTimeoutMs);
        entryLockTimeoutMs = Math.max(1L, entryLockTimeoutMs);
        moduleLockTimeoutMs = Math.max(1L, moduleLockTimeoutMs);
    }
}
