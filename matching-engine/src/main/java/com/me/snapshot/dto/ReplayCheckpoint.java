package com.me.snapshot.dto;

/**
 * 快照切点上某个 offset-seq topic 分区的下一个写入 offset。
 *
 * <p>account / order 两条 offset-seq 链路与各自模块的输入消费 offset、业务输出在同一个
 * Kafka 事务内提交；快照在 committer flush 完成后、持有模块锁的状态下读取端位点，
 * 因此该位点之前只包含本快照已覆盖的映射，之后只包含快照之后产生的映射。
 *
 * <p>恢复时从 {@code nextOffset} 开始读取 offset-seq 映射，再按 {@code seq} 过滤
 * 快照水位，避免每次都从 earliest 全量扫描历史映射。
 */
public record ReplayCheckpoint(
        String topic,
        int partition,
        long nextOffset) {
}
