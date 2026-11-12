package com.me.marketdata.service;

import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.entity.TradeRecord;
import com.me.shared.event.MarketDataEvent;
import java.util.Collections;
import java.util.List;

/**
 * 行情模块业务接口：消费成交行情，生成 K 线并归档到 QuestDB。
 */
public interface MarketDataService {

    /**
     * 批量处理一个 Kafka poll 批次：只归档本批最后 20 条成交明细，再用批内全部成交聚合 K 线。
     *
     * <p>同一 {@code (symbol, interval, bucket)} 的 K 线首次出现时立即写入，之后每
     * {@code app.marketdata.kline-write-every-batches} 个批次更新一次；跨 bucket 时写入
     * 上一根的最终快照。这样同一分钟只有“首次 + 每 N 批 + 收尾”几次写入。
     *
     * <p>分区 + offset 是 K 线的幂等水位：同一条 Kafka 记录重投时跳过已聚合的周期。
     *
     * @param records 同一批次内的行情记录（按消费顺序）
     */
    void ingestBatch(List<MarketDataRecord> records);

    /**
     * 管理端清空 K 线 / 锚点后调用：丢弃内存里的写入节流与 dirty 状态，
     * 避免重建后的第一个 bucket 被当成“已写过”而跳过。
     */
    void clearWriteState();

    /**
     * 处理一条行情记录的便捷方法，等价于只含一条记录的 {@link #ingestBatch(List)}。
     *
     * @param trade     行情记录（一笔成交）
     * @param partition 行情 topic 分区
     * @param offset    行情 topic offset
     */
    default void ingest(MarketDataEvent trade, int partition, long offset) {
        ingestBatch(Collections.singletonList(new MarketDataRecord(trade, partition, offset)));
    }

    /**
     * 查询最近 N 条 K 线，按 bucket 起点升序返回。
     *
     * @param symbol   交易对，如 {@code BTC-USDT}
     * @param interval K 线周期
     * @param limit    条数，1..1000，超出范围按边界夹取
     */
    List<Kline> klines(String symbol, KlineInterval interval, int limit);

    /**
     * 查询最近 N 笔成交，按成交时间倒序返回（最新在前）。
     *
     * @param symbol 交易对，如 {@code BTC-USDT}
     * @param limit  条数，1..500，超出范围按边界夹取
     */
    List<TradeRecord> recentTrades(String symbol, int limit);
}
