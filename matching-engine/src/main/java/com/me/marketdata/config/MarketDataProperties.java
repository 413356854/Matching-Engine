package com.me.marketdata.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 行情模块配置（{@code app.marketdata.*}）。
 *
 * @param klineIntervals     需要聚合的 K 线周期 label，例如 {@code 1m,5m,15m,30m,1h,12h,1d,1M}；
 *                           缺省仅 1m。label 大小写敏感：{@code 1m} 为分钟，{@code 1M} 为自然月
 * @param klineFlushIntervalMs 成交明细与 K 线内存快照批量落库的间隔（毫秒），默认 1000。
 *                           消费线程只更新内存缓存 / 分钟线，由后台线程按该间隔把待写成交与
 *                           有变化的 K 线快照写入 QuestDB；进程崩溃最多丢失这个间隔内的增量
 */
@ConfigurationProperties(prefix = "app.marketdata")
public record MarketDataProperties(
        List<String> klineIntervals,
        @DefaultValue("1000") long klineFlushIntervalMs) {

    public MarketDataProperties {
        klineIntervals = (klineIntervals == null || klineIntervals.isEmpty())
                ? List.of("1m")
                : List.copyOf(klineIntervals);
        if (klineFlushIntervalMs < 1) {
            throw new IllegalArgumentException(
                    "app.marketdata.kline-flush-interval-ms must be >= 1: "
                            + klineFlushIntervalMs);
        }
    }
}
