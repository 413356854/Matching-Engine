package com.me.snapshot;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 启动恢复配置：加载快照并把快照切点之后已提交的输入重放到内存。
 *
 * <p>默认关闭，保持“空状态启动”的既有行为；开启后恢复失败会阻止 consumer 启动。
 */
@ConfigurationProperties("app.snapshot.recovery")
public record SnapshotRecoveryProperties(
        /** 是否在启动阶段执行快照恢复。 */
        @DefaultValue("false") boolean enabled,
        /** 指定 snapshot_backup.id；0 表示取最新一条。 */
        @DefaultValue("0") long backupId,
        /** 非空时直接读取该文件，优先于 backupId，用于 MySQL 元数据丢失的灾备场景。 */
        @DefaultValue("") String file,
        /** 开启恢复但找不到可用快照时是否阻止启动。 */
        @DefaultValue("true") boolean failIfMissing,
        /** 严格校验 [快照 offset, committed offset) 内可见记录与 offset-seq 映射完全一致。 */
        @DefaultValue("true") boolean strictValidation,
        /** 一次恢复最多允许的 offset-seq 映射条数，超出要求重新做快照。 */
        @DefaultValue("5000000") int maxMappings,
        /** 单次抓取原始输入记录 / 校验区间的超时。 */
        @DefaultValue("60000") long fetchTimeoutMs,
        /** 修改消费组 offset 的超时（含旧成员过期等待）。 */
        @DefaultValue("60000") long offsetTimeoutMs,
        /** Kafka admin 单次请求超时。 */
        @DefaultValue("30000") long adminTimeoutMs) {

    public SnapshotRecoveryProperties {
        file = file == null ? "" : file.strip();
        maxMappings = Math.max(1, maxMappings);
        fetchTimeoutMs = Math.max(1L, fetchTimeoutMs);
        offsetTimeoutMs = Math.max(1L, offsetTimeoutMs);
        adminTimeoutMs = Math.max(1L, adminTimeoutMs);
    }
}
