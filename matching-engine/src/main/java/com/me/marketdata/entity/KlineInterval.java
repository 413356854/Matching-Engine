package com.me.marketdata.entity;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * K 线周期。
 *
 * <p>周期按 UTC epoch 毫秒向下取整对齐（{@link Math#floorDiv}），例如 1m 的 10:00:37
 * 属于 10:00:00 这个 bucket。
 */
public enum KlineInterval {

    ONE_MINUTE("1m", Duration.ofMinutes(1)),
    FIVE_MINUTES("5m", Duration.ofMinutes(5)),
    FIFTEEN_MINUTES("15m", Duration.ofMinutes(15)),
    THIRTY_MINUTES("30m", Duration.ofMinutes(30)),
    ONE_HOUR("1h", Duration.ofHours(1)),
    FOUR_HOURS("4h", Duration.ofHours(4)),
    ONE_DAY("1d", Duration.ofDays(1));

    private final String label;
    private final Duration span;

    KlineInterval(String label, Duration span) {
        this.label = label;
        this.span = span;
    }

    public String label() {
        return label;
    }

    /** 周期长度，用于计算相邻 bucket。 */
    public Duration span() {
        return span;
    }

    /** 事件时间所在的 bucket 起点。 */
    public Instant bucketStart(Instant time) {
        long spanMillis = span.toMillis();
        return Instant.ofEpochMilli(Math.floorDiv(time.toEpochMilli(), spanMillis) * spanMillis);
    }

    /** 按配置里的 label 解析；未知周期在启动时报错。 */
    public static KlineInterval of(String label) {
        return Arrays.stream(values())
                .filter(interval -> interval.label.equals(label))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unsupported kline interval: " + label + ", supported: " + supportedLabels()));
    }

    private static String supportedLabels() {
        return Arrays.stream(values())
                .map(KlineInterval::label)
                .collect(Collectors.joining(","));
    }
}
