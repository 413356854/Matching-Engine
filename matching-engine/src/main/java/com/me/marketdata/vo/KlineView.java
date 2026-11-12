package com.me.marketdata.vo;

import com.me.marketdata.entity.Kline;
import java.time.Instant;

/**
 * K 线视图：对外暴露的单条 K 线快照。
 *
 * <p>价格、数量、金额都是交易对最小单位的整数；{@code bucketStart} 是该 K 线周期的起点。
 */
public record KlineView(
        String symbol,
        String interval,
        Instant bucketStart,
        long openPrice,
        long highPrice,
        long lowPrice,
        long closePrice,
        long volume,
        long amount,
        long tradeCount) {

    public static KlineView from(Kline kline) {
        return new KlineView(
                kline.symbol(),
                kline.interval().label(),
                kline.bucketStart(),
                kline.openPrice(),
                kline.highPrice(),
                kline.lowPrice(),
                kline.closePrice(),
                kline.volume(),
                kline.amount(),
                kline.tradeCount());
    }
}
