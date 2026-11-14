package com.me.marketdata.service.impl;

import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineInterval;
import com.me.shared.event.MatchPairEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按 {@code (symbol, interval)} 维护当前 K 线的聚合器。
 *
 * <p>聚合器只持有内存状态，不接触 Spring / QuestDB：写入成功后再由调用方
 * {@link #commit(Kline)}，因此写库失败重试时内存不会领先于持久化结果。
 *
 * <p>同一 symbol 的行情由 Kafka 同一分区单线程消费，因此单条 K 线的“读当前值 → 计算 → 提交”
 * 在调用方是串行的；这里仍用 {@link ConcurrentHashMap} 保证跨线程可见性。
 *
 * <p>价格与成交量都是交易对最小单位的整数。
 */
public final class KlineAggregator {

    private final List<KlineInterval> intervals;
    private final ConcurrentHashMap<Key, Kline> current = new ConcurrentHashMap<>();

    public KlineAggregator(List<KlineInterval> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            throw new IllegalArgumentException("at least one kline interval is required");
        }
        this.intervals = List.copyOf(intervals);
    }

    /** 需要聚合的周期，按配置顺序。 */
    public List<KlineInterval> intervals() {
        return intervals;
    }

    /** 当前内存中的 K 线；进程刚启动或尚未处理过该 symbol 时为 {@code null}。 */
    public Kline current(String symbol, KlineInterval interval) {
        return current.get(new Key(symbol, interval));
    }

    /** 持久化成功后再提交内存状态。 */
    public void commit(Kline kline) {
        current.put(new Key(kline.symbol(), kline.interval()), kline);
    }

    /**
     * 计算事件对应的下一个快照，不修改状态。
     *
     * <p>事件进入新 bucket 且存在上一根 K 线时，新 K 线的 open 取上一根的 close；只有新线
     * 第一根（{@code currentCandle} 为空）才以成交价开盘。
     *
     * @param currentCandle 当前 K 线，可为 {@code null}（新 symbol / 新 bucket）
     * @return 下一个快照；事件早于当前 bucket（迟到成交）时返回 {@code null}
     */
    public Kline next(Kline currentCandle, MatchPairEvent trade,
                      KlineInterval interval, Instant bucketStart,
                      int partition, long offset) {
        if (currentCandle == null) {
            return Kline.opened(trade, interval, bucketStart, partition, offset);
        }
        if (currentCandle.bucketStart().isBefore(bucketStart)) {
            // 续线：新柱子以上一根的 close 作为 open，价格序列首尾相接。
            return Kline.opened(trade, interval, bucketStart, currentCandle.closePrice(),
                    partition, offset);
        }
        if (currentCandle.bucketStart().isAfter(bucketStart)) {
            return null;
        }
        // 同毫秒 / 同 executedAt 的成交按消费到达顺序更新，不用时间比较决定 close。
        return currentCandle.updated(trade, partition, offset);
    }

    /**
     * 当前 K 线之后、{@code bucketStart} 之前没有成交的 bucket 的平线 K 线。
     *
     * <p>每根平线的 OHLC 都沿用上一根的收盘价，量 / 额 / 笔数为 0；{@code current} 为空或
     * 不早于目标 bucket 时返回空列表。
     */
    public List<Kline> flatsUntil(Kline current, KlineInterval interval, Instant bucketStart,
                                  int partition, long offset) {
        if (current == null || !current.bucketStart().isBefore(bucketStart)) {
            return List.of();
        }
        List<Kline> flats = new ArrayList<>();
        Instant cursor = current.bucketStart().plus(interval.span());
        while (cursor.isBefore(bucketStart)) {
            flats.add(Kline.flat(current.symbol(), interval, cursor, current.closePrice(),
                    partition, offset));
            cursor = cursor.plus(interval.span());
        }
        return flats;
    }

    /** 清空内存状态；管理端清空 K 线后调用，避免内存领先于已删除的持久化数据。 */
    public void clear() {
        current.clear();
    }

    private record Key(String symbol, KlineInterval interval) {
    }
}
