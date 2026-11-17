package com.me.websocket.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * WebSocket 盘口推送配置（{@code app.websocket.*}）。
 *
 * @param pushIntervalMs        服务端读取订单簿并推送的间隔，默认 500ms
 * @param defaultDepth          客户端未指定 {@code depth} 时买卖两边各返回的价位数量
 * @param maxDepth              单边价位数量上限，超过按该值截断
 * @param allowedOriginPatterns 允许的握手 Origin，逗号分隔；{@code *} 表示不限制
 * @param sendTimeLimitMs       单次发送超时；超时后断开该慢连接
 * @param sendBufferSizeBytes   单连接发送缓冲上限；积压超过该值时断开该慢连接
 */
@ConfigurationProperties(prefix = "app.websocket")
public record WebSocketProperties(
        @DefaultValue("500") int pushIntervalMs,
        @DefaultValue("20") int defaultDepth,
        @DefaultValue("100") int maxDepth,
        @DefaultValue("*") String allowedOriginPatterns,
        @DefaultValue("5000") int sendTimeLimitMs,
        @DefaultValue("524288") int sendBufferSizeBytes) {

    public WebSocketProperties {
        if (pushIntervalMs < 50) {
            throw new IllegalArgumentException(
                    "app.websocket.push-interval-ms must be >= 50: " + pushIntervalMs);
        }
        if (maxDepth < 1) {
            throw new IllegalArgumentException(
                    "app.websocket.max-depth must be >= 1: " + maxDepth);
        }
        if (defaultDepth < 1 || defaultDepth > maxDepth) {
            throw new IllegalArgumentException(
                    "app.websocket.default-depth must be in [1, " + maxDepth + "]: "
                            + defaultDepth);
        }
        if (allowedOriginPatterns == null || allowedOriginPatterns.isBlank()) {
            allowedOriginPatterns = "*";
        }
        if (sendTimeLimitMs < 1) {
            throw new IllegalArgumentException(
                    "app.websocket.send-time-limit-ms must be >= 1: " + sendTimeLimitMs);
        }
        if (sendBufferSizeBytes < 1024) {
            throw new IllegalArgumentException(
                    "app.websocket.send-buffer-size-bytes must be >= 1024: "
                            + sendBufferSizeBytes);
        }
    }
}
