package com.me.marketdata.entity;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * K 线周期。
 *
 * <p>固定长度周期按 UTC epoch 毫秒向下取整对齐（{@link Math#floorDiv}），例如 1m 的
 * 10:00:37 属于 10:00:00 这个 bucket；12h 的 13:00 属于 12:00 这个 bucket。
 *
 * <p>{@code 1M} 是自然月，不能按固定毫秒数计算：bucket 起点是 UTC 每月 1 日 00:00，
 * 相邻 bucket 长度随月份为 28~31 天。注意 label 大小写敏感：{@code 1m} 是 1 分钟，
 * {@code 1M} 是 1 个月。
 */
public enum KlineInterval {

    ONE_MINUTE("1m", Duration.ofMinutes(1)),
    FIVE_MINUTES("5m", Duration.ofMinutes(5)),
    FIFTEEN_MINUTES("15m", Duration.ofMinutes(15)),
    THIRTY_MINUTES("30m", Duration.ofMinutes(30)),
    ONE_HOUR("1h", Duration.ofHours(1)),
    FOUR_HOURS("4h", Duration.ofHours(4)),
    TWELVE_HOURS("12h", Duration.ofHours(12)),
    ONE_DAY("1d", Duration.ofDays(1)),
    /** 自然月；{@link #span} 为 {@code null}，按 UTC 日历月推进。 */
    ONE_MONTH("1M", null);

    private final String label;
    private final Duration span;

    KlineInterval(String label, Duration span) {
        this.label = label;
        this.span = span;
    }

    public String label() {
        return label;
    }

    /** 事件时间所在的 bucket 起点；1M 对齐到 UTC 每月 1 日 00:00。 */
    public Instant bucketStart(Instant time) {
        if (span == null) {
            return time.atZone(ZoneOffset.UTC)
                    .toLocalDate()
                    .withDayOfMonth(1)
                    .atStartOfDay(ZoneOffset.UTC)
                    .toInstant();
        }
        long spanMillis = span.toMillis();
        return Instant.ofEpochMilli(Math.floorDiv(time.toEpochMilli(), spanMillis) * spanMillis);
    }

    /**
     * 紧跟在 {@code bucketStart} 之后的下一个 bucket 起点。
     *
     * <p>固定长度周期直接加跨度；1M 用日历月推进（1 月 1 日 → 2 月 1 日），不受月份天数
     * 影响。调用方传入的必须是本周期对齐后的 bucket 起点。
     */
    public Instant nextBucket(Instant bucketStart) {
        return span == null
                ? bucketStart.atZone(ZoneOffset.UTC).plusMonths(1).toInstant()
                : bucketStart.plus(span);
    }

    /** 按配置里的 label 解析（大小写敏感）；未知周期在启动时报错。 */
    public static KlineInterval of(String label) {
        String normalized = label == null ? "" : label.strip();
        return Arrays.stream(values())
                .filter(interval -> interval.label.equals(normalized))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unsupported kline interval: " + label + ", supported: "
                                + supportedLabels()));
    }

    private static String supportedLabels() {
        return Arrays.stream(values())
                .map(KlineInterval::label)
                .collect(Collectors.joining(","));
    }
}
