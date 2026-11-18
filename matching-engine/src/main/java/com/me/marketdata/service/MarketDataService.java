package com.me.marketdata.service;

import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.entity.TradeRecord;
import com.me.shared.event.MatchPairEvent;
import java.util.Collections;
import java.util.List;

/**
 * 行情模块业务接口：消费撮合对，生成 K 线并归档到 QuestDB。
 */
public interface MarketDataService {

    /** 每个交易对保留 / 展示的最新成交条数，也是单个消费批次归档成交的上限。 */
    int RECENT_TRADES_LIMIT = 50;

    /**
     * 批量处理一个 Kafka poll 批次：把本批最后 {@value #RECENT_TRADES_LIMIT} 条成交推进
     * 内存缓存并放入待写队列，再用批内全部成交更新进程内的当前分钟线。
     *
     * <p>成交明细与 K 线都不在消费线程落库：成交队列和每个 bucket 的最新 K 线快照先放进
     * 内存，由后台线程按 {@code app.marketdata.kline-flush-interval-ms}（默认 1000ms）
     * 整批刷新到 QuestDB。进程崩溃时最多丢失一个刷新间隔内已提交 offset 的增量。
     *
     * <p>逐笔只更新分钟线；分钟线完成后由后台聚合到更高周期。
     * 分区 + offset 是 K 线的幂等水位：同一条 Kafka 记录重投时跳过已聚合的周期。
     *
     * @param records 同一批次内的行情记录（按消费顺序）
     */
    void ingestBatch(List<MarketDataRecord> records);

    /**
     * 管理端清空 K 线 / 锚点后调用：丢弃内存里待刷新的快照与锚点状态，
     * 避免清空后又被旧的刷新批次写回。
     */
    void clearWriteState();

    /**
     * 处理一条行情记录的便捷方法，等价于只含一条记录的 {@link #ingestBatch(List)}。
     *
     * @param trade     撮合对记录（一笔成交）
     * @param partition 撮合对 topic 分区
     * @param offset    撮合对 topic offset
     */
    default void ingest(MatchPairEvent trade, int partition, long offset) {
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
     * 查询各周期当前正在聚合的最新 K 线（只读进程内内存，不查 QuestDB）。
     *
     * <p>返回顺序与配置的 {@code app.marketdata.kline-intervals} 一致；尚未产生成交的周期
     * 不返回。内存快照最多领先已落库数据一个刷新间隔，适合给 WebSocket 做实时推送：
     * 前端按时间戳决定覆盖最后一根还是追加新柱子。
     *
     * @param symbol 交易对，如 {@code BTC-USDT}
     */
    List<Kline> latestKlines(String symbol);

    /**
     * 查询最近 N 笔成交，按成交时间倒序返回（最新在前）。
     *
     * <p>只读每个交易对的内存缓存，不查询 QuestDB；{@code limit} 超过
     * {@value #RECENT_TRADES_LIMIT} 时按该上限截断。
     *
     * @param symbol 交易对，如 {@code BTC-USDT}
     * @param limit  条数，1..{@value #RECENT_TRADES_LIMIT}
     */
    List<TradeRecord> recentTrades(String symbol, int limit);
}
