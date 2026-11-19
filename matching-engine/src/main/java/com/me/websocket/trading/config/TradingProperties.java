package com.me.websocket.trading.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * WebSocket 交易端点配置（{@code app.websocket.trading.*}）。
 *
 * @param enabled             是否启用 /ws/trade；false 时不注册端点
 * @param path                端点路径，默认 /ws/trade
 * @param allowedOriginPatterns 握手 Origin 白名单，逗号分隔；* 表示不限制
 * @param maxOrdersPerSecond  单连接每秒最多提交的下单/撤单请求数；<=0 表示不限制
 * @param maxPendingRequests  单连接同时等待 ack 的最大请求数
 * @param ackTimeoutMs        等待 ingress ack 的超时；超时后客户端应查单
 * @param ackSweepIntervalMs  超时扫描间隔
 * @param ackBatchSize        二进制连接单个 AckBatch 帧最多携带的 ack 条数
 * @param maxMessageBytes     单条客户端 / 服务端消息上限（二进制帧同样受限）
 * @param instanceId          多 WS 实例部署时用于生成 connectionId 前缀
 */
@ConfigurationProperties(prefix = "app.websocket.trading")
public record TradingProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("/ws/trade") String path,
        @DefaultValue("*") String allowedOriginPatterns,
        @DefaultValue("50") int maxOrdersPerSecond,
        @DefaultValue("100") int maxPendingRequests,
        @DefaultValue("3000") int ackTimeoutMs,
        @DefaultValue("1000") int ackSweepIntervalMs,
        @DefaultValue("64") int ackBatchSize,
        @DefaultValue("8192") int maxMessageBytes,
        @DefaultValue("1") int instanceId) {

    public TradingProperties {
        if (path == null || path.isBlank() || !path.startsWith("/")) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.path must start with '/': " + path);
        }
        if (allowedOriginPatterns == null || allowedOriginPatterns.isBlank()) {
            allowedOriginPatterns = "*";
        }
        if (maxOrdersPerSecond < 0) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.max-orders-per-second must be >= 0: "
                            + maxOrdersPerSecond);
        }
        if (maxPendingRequests < 1) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.max-pending-requests must be >= 1: "
                            + maxPendingRequests);
        }
        if (ackTimeoutMs < 100) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.ack-timeout-ms must be >= 100: " + ackTimeoutMs);
        }
        if (ackSweepIntervalMs < 100) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.ack-sweep-interval-ms must be >= 100: "
                            + ackSweepIntervalMs);
        }
        if (ackBatchSize < 1 || ackBatchSize > 1024) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.ack-batch-size must be in [1, 1024]: "
                            + ackBatchSize);
        }
        if (maxMessageBytes < 1024) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.max-message-bytes must be >= 1024: "
                            + maxMessageBytes);
        }
        if (instanceId < 0) {
            throw new IllegalArgumentException(
                    "app.websocket.trading.instance-id must be >= 0: " + instanceId);
        }
    }
}
