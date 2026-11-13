package com.me.currency.ingress;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 下单命令入口（Disruptor）配置。
 *
 * @param ringSize        ring 槽位数，必须是 2 的幂；HTTP 线程非阻塞入队，满时返回 503
 * @param batchSize       单个 symbol 的待发送订单达到该数量后立即 flush 整个 map
 * @param flushIntervalMs 聚合时间窗口：定时 tick 到达时 flush 整个 map
 */
@ConfigurationProperties("app.currency.ingress")
public record OrderIngressProperties(
        @DefaultValue("65536") int ringSize,
        @DefaultValue("2000") int batchSize,
        @DefaultValue("50") long flushIntervalMs) {

    public OrderIngressProperties {
        if (ringSize <= 0 || Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException(
                    "app.currency.ingress.ring-size must be a positive power of two: "
                            + ringSize);
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException(
                    "app.currency.ingress.batch-size must be positive: " + batchSize);
        }
        if (flushIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "app.currency.ingress.flush-interval-ms must be positive: "
                            + flushIntervalMs);
        }
    }
}
