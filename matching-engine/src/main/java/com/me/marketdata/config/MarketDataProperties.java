package com.me.marketdata.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 行情模块配置（{@code app.marketdata.*}）。
 *
 * @param klineIntervals          需要聚合的 K 线周期 label，例如 {@code 1m,5m,1h}；缺省仅 1m
 * @param klineWriteEveryBatches  同一 bucket 的 K 线在首次写入后，每多少个批次再更新一次；
 *                                默认 5。bucket 结束时（下一分钟到来）会强制写入最终快照
 */
@ConfigurationProperties(prefix = "app.marketdata")
public record MarketDataProperties(
        List<String> klineIntervals,
        @DefaultValue("5") int klineWriteEveryBatches) {

    public MarketDataProperties {
        klineIntervals = (klineIntervals == null || klineIntervals.isEmpty())
                ? List.of("1m")
                : List.copyOf(klineIntervals);
        if (klineWriteEveryBatches < 1) {
            throw new IllegalArgumentException(
                    "app.marketdata.kline-write-every-batches must be >= 1: "
                            + klineWriteEveryBatches);
        }
    }
}
