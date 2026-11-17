package com.me.snapshot.dto;

/**
 * 快照文件的外层结构：格式版本 + payload 的 SHA-256 + payload。
 *
 * <p>校验和覆盖的是 {@code payload} 的 <b>紧凑 JSON</b> 字节（与文件是否 pretty-print
 * 无关）。恢复方应使用同样的 {@code JsonMapper} 重新序列化 payload 后比对，而不是
 * 直接对文件中的 payload 片段做哈希。
 */
public record SnapshotFile(
        int formatVersion,
        String checksum,
        SnapshotPayload payload) {

    /**
     * 当前文件格式版本；结构变化时递增。
     *
     * <p>版本 2 在 payload 中新增 {@code replayCheckpoints}：两个 offset-seq topic 在
     * 快照切点的逐分区端位点。
     */
    public static final int CURRENT_FORMAT_VERSION = 2;
}
