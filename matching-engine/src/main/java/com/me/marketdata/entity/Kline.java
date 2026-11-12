package com.me.marketdata.entity;

import com.me.shared.event.MarketDataEvent;
import java.time.Instant;

/**
 * 一条 K 线快照。
 *
 * <p>按 Kafka poll 批次重新计算完整快照写入 QuestDB（同一 bucket 只写最终快照），
 * {@code (bucketStart, symbol, interval)} 是幂等键；{@code sourcePartition/sourceOffset}
 * 记录已聚合到的最后一条 Kafka 记录，用于重投去重与进程重启后从 QuestDB 续算。
 *
 * <p>{@code open/high/low/close}、{@code volume}、{@code amount} 都是交易对最小单位的整数。
 */
public record Kline(
        String symbol,
        KlineInterval interval,
        Instant bucketStart,
        long openPrice,
        long highPrice,
        long lowPrice,
        long closePrice,
        long volume,
        long amount,
        long tradeCount,
        int sourcePartition,
        long sourceOffset) {

    /**
     * 新 bucket 的第一笔成交，{@code openPrice} 由调用方指定（续线取上一根 K 线的 close）。
     *
     * <p>high/low 同时覆盖 open 与首笔成交价，保证 OHLC 始终有效（如上一根 close 高于
     * 首笔成交价时 low 不会高于 open）。成交量为该笔成交量，笔数为 1。
     */
    public static Kline opened(MarketDataEvent trade, KlineInterval interval, Instant bucketStart,
                               long openPrice, int partition, long offset) {
        long price = trade.price();
        return new Kline(trade.symbol(), interval, bucketStart,
                openPrice,
                Math.max(openPrice, price),
                Math.min(openPrice, price),
                price,
                trade.quantity(), trade.amount(), 1, partition, offset);
    }

    /** 新线的第一根 K 线：没有上一根 close，四个价格都取成交价。 */
    public static Kline opened(MarketDataEvent trade, KlineInterval interval, Instant bucketStart,
                               int partition, long offset) {
        return opened(trade, interval, bucketStart, trade.price(), partition, offset);
    }

    /**
     * 没有成交的 bucket：OHLC 全部沿用上一根 K 线的收盘价，量 / 额 / 笔数为 0。
     *
     * <p>{@code sourcePartition/sourceOffset} 记录触发补线的 Kafka 记录，重投时与其它快照一样
     * 参与幂等判断。
     */
    public static Kline flat(String symbol, KlineInterval interval, Instant bucketStart,
                             long price, int partition, long offset) {
        return new Kline(symbol, interval, bucketStart, price, price, price, price, 0, 0, 0,
                partition, offset);
    }

    /** 同一 bucket 的后续成交：high/low 取极值，close 取最新成交价，量 / 额 / 笔数累加。 */
    public Kline updated(MarketDataEvent trade, int partition, long offset) {
        return new Kline(symbol, interval, bucketStart,
                openPrice,
                Math.max(highPrice, trade.price()),
                Math.min(lowPrice, trade.price()),
                trade.price(),
                Math.addExact(volume, trade.quantity()),
                Math.addExact(amount, trade.amount()),
                Math.addExact(tradeCount, 1),
                partition, offset);
    }

    /** 该快照是否已经包含指定分区上的这条记录（重投 / 重启重放时跳过）。 */
    public boolean alreadyIncludes(int partition, long offset) {
        return sourcePartition == partition && sourceOffset >= offset;
    }
}
