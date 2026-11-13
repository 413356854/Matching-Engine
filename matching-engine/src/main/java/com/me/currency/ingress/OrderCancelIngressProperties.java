package com.me.currency.ingress;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 撤单入口（独立 Disruptor）配置。
 *
 * @param ringSize        ring 槽位数，必须是 2 的幂；HTTP 线程非阻塞入队，满时返回 503
 * @param batchSize       单个 symbol 的待发送撤单达到该数量后立即 flush 整个 map
 * @param flushIntervalMs 聚合时间窗口：定时 tick 到达时 flush 整个 map
 */
@ConfigurationProperties("app.currency.cancel-ingress")
public record OrderCancelIngressProperties(
        @DefaultValue("65536") int ringSize,
        @DefaultValue("1000") int batchSize,
        @DefaultValue("20") long flushIntervalMs) {

    public OrderCancelIngressProperties {
        if (ringSize <= 0 || Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException(
                    "app.currency.cancel-ingress.ring-size must be a positive power of two: "
                            + ringSize);
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException(
                    "app.currency.cancel-ingress.batch-size must be positive: " + batchSize);
        }
        if (flushIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "app.currency.cancel-ingress.flush-interval-ms must be positive: "
                            + flushIntervalMs);
        }
    }
}
