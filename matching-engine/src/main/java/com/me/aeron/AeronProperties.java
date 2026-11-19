package com.me.aeron;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Aeron IPC 通道配置（{@code app.aeron.*}）。
 *
 * <p>当前 WS 交易端点和币币模块在同一个 Spring Boot 进程内，使用内嵌 media driver 和
 * {@code aeron:ipc}。后续 WS 独立部署时，把 {@code embedded-driver} 改为 false 并在
 * 两个进程间共享 {@code dir}，或把 {@code channel} 换成 {@code aeron:udp?...}。
 *
 * @param enabled         是否启用 Aeron；false 时不启动 media driver，也不注册 /ws/trade
 * @param dir             media driver 目录；相对路径按进程工作目录解析
 * @param channel         Aeron channel；同机 IPC 为 {@code aeron:ipc}
 * @param requestStreamId WS → 币币的请求 stream
 * @param ackStreamId     币币 → WS 的 ack stream
 * @param embeddedDriver  是否由本进程启动 media driver；false 表示连接已有 driver
 * @param dirDeleteOnStart 启动内嵌 driver 时是否清空目录；共享 driver 时必须为 false
 * @param termBufferLength term buffer 长度，必须是 2 的幂
 * @param maxMessageBytes 单条消息上限
 */
@ConfigurationProperties(prefix = "app.aeron")
public record AeronProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("./target/aeron") String dir,
        @DefaultValue("aeron:ipc") String channel,
        @DefaultValue("1001") int requestStreamId,
        @DefaultValue("1002") int ackStreamId,
        @DefaultValue("true") boolean embeddedDriver,
        @DefaultValue("true") boolean dirDeleteOnStart,
        @DefaultValue("16777216") int termBufferLength,
        @DefaultValue("16384") int maxMessageBytes) {

    private static final int MIN_TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int MIN_MAX_MESSAGE_BYTES = 1024;

    public AeronProperties {
        if (dir == null || dir.isBlank()) {
            throw new IllegalArgumentException("app.aeron.dir must not be blank");
        }
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("app.aeron.channel must not be blank");
        }
        if (requestStreamId <= 0 || ackStreamId <= 0) {
            throw new IllegalArgumentException(
                    "app.aeron request/ack stream ids must be positive: request="
                            + requestStreamId + ", ack=" + ackStreamId);
        }
        if (requestStreamId == ackStreamId) {
            throw new IllegalArgumentException(
                    "app.aeron request and ack stream ids must differ: " + requestStreamId);
        }
        if (termBufferLength < MIN_TERM_BUFFER_LENGTH
                || Integer.bitCount(termBufferLength) != 1) {
            throw new IllegalArgumentException(
                    "app.aeron.term-buffer-length must be a power of two >= "
                            + MIN_TERM_BUFFER_LENGTH + ": " + termBufferLength);
        }
        if (maxMessageBytes < MIN_MAX_MESSAGE_BYTES || maxMessageBytes > termBufferLength) {
            throw new IllegalArgumentException(
                    "app.aeron.max-message-bytes must be in ["
                            + MIN_MAX_MESSAGE_BYTES + ", " + termBufferLength + "]: "
                            + maxMessageBytes);
        }
    }
}
