package com.me.marketdata.service.impl;

import com.me.marketdata.config.MarketDataProperties;
import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineAnchor;
import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.entity.TradeRecord;
import com.me.marketdata.repository.MarketDataRepository;
import com.me.marketdata.service.MarketDataRecord;
import com.me.marketdata.service.MarketDataService;
import com.me.shared.event.MatchPairEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 行情归档实现：按 Kafka poll 批次批量写成交明细，K 线在进程内聚合后按固定间隔批量落库。
 *
 * <p><b>1 秒刷新</b>：消费线程收到成交时只更新 {@link KlineAggregator} 里的当前 K 线，并把
 * 最新快照放进 {@link #pendingKlines}；独立的后台线程每
 * {@code app.marketdata.kline-flush-interval-ms}（默认 1000ms）把有待更新的 bucket 整批写入
 * QuestDB，同一 bucket 只写最新一份。这样行情消费路径不做 K 线落库 I/O，写入次数也从
 * “每批一次”降到“每秒每个有成交的 bucket 一次”。
 *
 * <p>代价是数据库里的当前 K 线最多滞后一个刷新间隔；进程崩溃时，已提交 Kafka offset 但还没
 * 刷新的最多 1 秒 K 线增量会丢失。成交明细 {@code md_trade} 仍按批次同步写入，不受影响；
 * 需要精确口径时应以成交明细重算。
 *
 * <p><b>锚点与补线</b>：每个 {@code (symbol, interval)} 在 {@code md_kline_anchor} 里有一行
 * 锚点，记录已经计算到的最新 bucket。开始计算前先确认当前位置（内存未命中时才读库）：
 * <ul>
 *   <li>没有锚点：说明是新线，以当前成交新建 K 线；</li>
 *   <li>锚点早于事件 bucket：中间没有成交的 bucket 补成平线（OHLC 沿用上一根收盘价，量为 0），
 *       再把锚点推进到事件 bucket；</li>
 *   <li>锚点等于事件 bucket：取出已有 K 线更新。</li>
 * </ul>
 * 锚点晚于事件 bucket 的迟到成交不参与 K 线计算。补线只针对开始消费之后的缺口，锚点之前
 * 没有 K 线的历史区间不回溯生成。刷新线程只在 K 线写成功后才推进锚点，因此锚点永远不会
 * 领先于已落库的快照。
 *
 * <p><b>成交明细</b>：每个 poll 批次只归档最后 {@value #TRADE_ARCHIVE_LIMIT} 条成交，
 * 避免 {@code md_trade} 随订单量线性膨胀；归档前先推进每个交易对的最新成交内存缓存，
 * 接口与 WebSocket 推送都读缓存。K 线仍然使用批内全部成交聚合。
 *
 * <p><b>幂等与重启</b>：整批成交先批量写 {@code md_trade}（QuestDB DEDUP 去重），再在本地
 * 状态上计算各周期 K 线。K 线快照带 {@code (source_partition, source_offset)} 水位，重投时
 * 已包含的记录直接跳过。写库失败时待写快照保留在内存，下一个刷新间隔重试；进程重启后按
 * 锚点 / 库里最新快照恢复聚合位置，最多丢失崩溃前一个刷新间隔内的 K 线增量。
 */
@Service
public class MarketDataServiceImpl implements MarketDataService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataServiceImpl.class);

    /** 每个 poll 批次最多归档的成交明细条数，与缓存 / 前端展示上限一致。 */
    static final int TRADE_ARCHIVE_LIMIT = MarketDataService.RECENT_TRADES_LIMIT;

    private static final String FLUSH_THREAD_NAME = "kline-flush";

    private final KlineAggregator aggregator;
    private final MarketDataRepository repository;
    private final RecentTradeCache tradeCache;
    private final long flushIntervalMs;

    /**
     * 待落库的最新 K 线快照，key 为 {@code (symbol, interval, bucketStart)}。
     *
     * <p>消费线程只做 put（同一 bucket 后写覆盖先写），刷新线程整批写入。
     * ConcurrentHashMap 保证跨线程可见性；刷新线程只删除“仍是本次写入那一份”的条目，
     * 避免刷新期间消费线程写入的新快照被误删。
     */
    private final ConcurrentHashMap<KlineBucketKey, Kline> pendingKlines =
            new ConcurrentHashMap<>();

    /** 已落库 K 线对应的锚点，等刷新线程写入；只在 {@link #flushMonitor} 内访问。 */
    private final Map<KlineKey, Instant> pendingAnchors = new HashMap<>();

    /** 已经成功写入的锚点 bucket，避免每个刷新间隔重复覆盖同一行。 */
    private final Map<KlineKey, Instant> writtenAnchors = new HashMap<>();

    /** 串行化刷新与管理端清空，避免清空 K 线后又被旧的刷新批次写回。 */
    private final Object flushMonitor = new Object();

    private volatile ScheduledExecutorService flusher;

    @Autowired
    public MarketDataServiceImpl(KlineAggregator aggregator, MarketDataRepository repository,
                                 RecentTradeCache tradeCache, MarketDataProperties properties) {
        this(aggregator, repository, tradeCache, properties.klineFlushIntervalMs());
    }

    /** 测试用：直接指定刷新间隔（毫秒）。测试不会启动后台线程，需手动调用 flushPending。 */
    MarketDataServiceImpl(KlineAggregator aggregator, MarketDataRepository repository,
                          RecentTradeCache tradeCache, long flushIntervalMs) {
        if (flushIntervalMs < 1) {
            throw new IllegalArgumentException("flushIntervalMs must be >= 1: " + flushIntervalMs);
        }
        this.aggregator = aggregator;
        this.repository = repository;
        this.tradeCache = tradeCache;
        this.flushIntervalMs = flushIntervalMs;
    }

    /** 启动 K 线刷新线程；固定延迟调度，单线程，不与其他模块的定时任务抢线程。 */
    @PostConstruct
    void startFlusher() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, FLUSH_THREAD_NAME);
            thread.setDaemon(true);
            return thread;
        });
        flusher = executor;
        executor.scheduleWithFixedDelay(this::flushSafely,
                flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** 停机前再刷一次，尽量不丢最后一个间隔内的快照；宕机场景仍会丢最多一个间隔。 */
    @PreDestroy
    void stopFlusher() {
        ScheduledExecutorService executor = flusher;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
        flushSafely();
    }

    @Override
    public void clearWriteState() {
        synchronized (flushMonitor) {
            pendingKlines.clear();
            pendingAnchors.clear();
            writtenAnchors.clear();
        }
    }

    @Override
    public void ingestBatch(List<MarketDataRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        for (MarketDataRecord record : records) {
            MatchPairEvent trade = record == null ? null : record.event();
            if (trade == null || trade.symbol() == null || trade.symbol().isBlank()) {
                throw new IllegalArgumentException("match pair trade and symbol are required");
            }
        }

        // 只归档本批最后 50 条成交；K 线仍使用全部 records 聚合。
        // 先推进内存缓存再落库：接口 / WebSocket 立即能看到最新成交；落库失败重投时
        // 缓存按 tradeId 去重，QuestDB 由 DEDUP 键兜底。
        List<MatchPairEvent> trades = records.stream().map(MarketDataRecord::event).toList();
        int tradeFrom = Math.max(0, trades.size() - TRADE_ARCHIVE_LIMIT);
        List<MatchPairEvent> archivedTrades =
                List.copyOf(trades.subList(tradeFrom, trades.size()));
        tradeCache.add(archivedTrades);
        repository.insertTrades(archivedTrades);

        // 本批次内先在本地状态上计算，全部成功后再合并到待刷新的内存缓存并提交给 aggregator；
        // 中途异常时 aggregator / pendingKlines 都不变，Kafka 整批重投不会重复累计。
        Map<KlineKey, Kline> working = new HashMap<>();
        Set<KlineKey> resolved = new HashSet<>();
        // 本批产生的最新快照；同一 bucket 只保留最终状态。
        Map<KlineBucketKey, Kline> batchPending = new LinkedHashMap<>();
        // 本批从库里恢复出的锚点 bucket；内存命中时不需要。锚点存在但快照缺失时，
        // 早于锚点的迟到成交不重新开线。
        Map<KlineKey, Instant> restoredAnchors = new HashMap<>();
        // 一条批量消息包含多笔成交，同一 symbol 的多笔成交共享 (partition, offset)。
        // 水位检查必须以整条 Kafka 记录为单位：首次遇到该记录的 symbol+interval+bucket 时
        // 判断是否已聚合，已聚合则跳过该记录内该 bucket 的全部成交；否则全部处理。不能逐笔
        // 比较 sourceOffset，否则同一条记录里的第二笔成交会被误判为重复。
        Set<KlineRecordKey> seenRecords = new HashSet<>();
        Set<KlineRecordKey> includedRecords = new HashSet<>();

        for (MarketDataRecord record : records) {
            MatchPairEvent trade = record.event();
            for (KlineInterval interval : aggregator.intervals()) {
                KlineKey key = new KlineKey(trade.symbol(), interval);
                Instant bucketStart = interval.bucketStart(trade.executedAt());
                KlineBucketKey bucketKey = new KlineBucketKey(trade.symbol(), interval, bucketStart);
                Kline current = working.get(key);
                if (!resolved.contains(key)) {
                    resolved.add(key);
                    current = aggregator.current(trade.symbol(), interval);
                    if (current == null) {
                        Restored restored = restoreFromStore(key, bucketStart,
                                record.partition(), record.offset(), batchPending);
                        if (restored.anchorBucket() != null) {
                            restoredAnchors.put(key, restored.anchorBucket());
                        }
                        current = restored.current();
                    }
                    if (current != null) {
                        working.put(key, current);
                    }
                }
                KlineRecordKey recordKey = new KlineRecordKey(
                        record.partition(), record.offset(), trade.symbol(), interval, bucketStart);
                if (seenRecords.add(recordKey)
                        && current != null
                        && current.bucketStart().equals(bucketStart)
                        && current.alreadyIncludes(record.partition(), record.offset())) {
                    includedRecords.add(recordKey);
                }
                if (includedRecords.contains(recordKey)) {
                    continue;
                }

                if (current == null) {
                    Instant anchorBucket = restoredAnchors.get(key);
                    if (anchorBucket != null && bucketStart.isBefore(anchorBucket)) {
                        continue;
                    }
                } else if (current.bucketStart().isBefore(bucketStart)) {
                    // 当前 K 线早于事件 bucket：中间没有成交的 bucket 补成平线。
                    // 当前 bucket 的最终快照无需在此强制写入——它的每次更新都已经放进
                    // pendingKlines，刷新线程会写入最新状态。
                    for (Kline flat : aggregator.flatsUntil(current, interval, bucketStart,
                            record.partition(), record.offset())) {
                        batchPending.put(new KlineBucketKey(
                                trade.symbol(), interval, flat.bucketStart()), flat);
                    }
                }

                Kline next = aggregator.next(current, trade, interval, bucketStart,
                        record.partition(), record.offset());
                if (next == null) {
                    continue;
                }
                working.put(key, next);
                batchPending.put(bucketKey, next);
            }
        }

        // 计算全部成功后一次性合并；此后由后台刷新线程按间隔落库。
        pendingKlines.putAll(batchPending);
        // 写库失败重试 / 进程重启时从 aggregator 或库里恢复，不会重复累计。
        working.values().forEach(aggregator::commit);
    }

    /**
     * 内存中没有当前 K 线时，从 QuestDB 恢复聚合位置。
     *
     * <p>有锚点：读取 {@code [锚点 bucket, 事件 bucket]} 内已有的 K 线。事件 bucket 已有快照
     * 则返回它，由调用方更新；事件 bucket 晚于锚点但还没有快照时，返回锚点之后最近一根 K 线
     * 作为“上一根”，让新 K 线沿用它的 close 作为 open；锚点之后、事件 bucket 之前没有成交的
     * bucket 生成平线快照放入 {@code pending}，已存在的快照不覆盖，因此锚点没来得及推进
     * （进程崩溃 / 写库失败）时重投也不会把真实 K 线冲成平线。事件 bucket 早于锚点时返回
     * 锚点 bucket 的快照，让迟到成交按“早于当前 bucket”处理。
     *
     * <p>没有锚点：锚点表刚上线 / 锚点丢失时库里可能已有历史 K 线，读取该
     * {@code (symbol, interval)} 最新的一根作为“上一根”。事件 bucket 更晚时由调用方补平
     * 中间的空柱并以它的 close 续线开盘；事件 bucket 相同则更新已有快照，更早则按迟到成交
     * 忽略。只有库里一根 K 线都没有时才是真正的新线。对应的 K 线刷新成功后由刷新线程
     * 推进锚点。
     */
    private Restored restoreFromStore(KlineKey key, Instant bucketStart,
                                      int partition, long offset,
                                      Map<KlineBucketKey, Kline> pending) {
        String symbol = key.symbol();
        KlineInterval interval = key.interval();
        KlineAnchor anchor = repository.findAnchor(symbol, interval);
        if (anchor == null) {
            Kline latest = repository.findLatestKline(symbol, interval);
            return new Restored(latest, null);
        }

        Instant anchorBucket = anchor.bucketStart();
        Instant upper = anchorBucket.isAfter(bucketStart) ? anchorBucket : bucketStart;
        List<Kline> existing = repository.findKlinesBetween(
                symbol, interval, anchorBucket, upper);
        Map<Instant, Kline> byBucket = new HashMap<>();
        for (Kline kline : existing) {
            // 同一 bucket 按 updated_at 倒序排列，保留第一行即最新快照。
            byBucket.putIfAbsent(kline.bucketStart(), kline);
        }

        Kline previous = null;
        for (Instant cursor = anchorBucket; cursor.isBefore(bucketStart);
             cursor = interval.nextBucket(cursor)) {
            Kline stored = byBucket.get(cursor);
            if (stored != null) {
                previous = stored;
            } else if (previous != null) {
                pending.put(new KlineBucketKey(symbol, interval, cursor),
                        Kline.flat(symbol, interval, cursor, previous.closePrice(),
                                partition, offset));
            }
        }
        Kline current;
        if (bucketStart.isBefore(anchorBucket)) {
            current = byBucket.get(anchorBucket);
        } else {
            current = byBucket.get(bucketStart);
            if (current == null && previous != null) {
                // 事件 bucket 还没有快照：返回最近一根已有 K 线作为“上一根”，
                // 新建 K 线时 open 取其 close，保证进程重启后续线规则不变。
                current = previous;
            }
        }
        return new Restored(current, anchorBucket);
    }

    /**
     * 把待落库的 K 线快照整批写入 QuestDB，成功后推进锚点。
     *
     * <p>每个 bucket 只写最新一份快照；写库失败时快照保留在内存，下一个刷新间隔重试。
     * 先写 K 线再写锚点，且只有 K 线写成功后才把对应 bucket 加入锚点，保证锚点不领先于
     * 已持久化的快照。包级可见便于单测直接触发刷新。
     */
    void flushPending() {
        synchronized (flushMonitor) {
            List<Map.Entry<KlineBucketKey, Kline>> snapshot =
                    new ArrayList<>(pendingKlines.entrySet());
            if (!snapshot.isEmpty()) {
                List<Kline> klines = new ArrayList<>(snapshot.size());
                for (Map.Entry<KlineBucketKey, Kline> entry : snapshot) {
                    klines.add(entry.getValue());
                }
                repository.insertKlines(klines);
                for (Kline kline : klines) {
                    // 只删除仍是本次写入的那份快照：刷新期间消费线程可能已经 put 了更新的版本，
                    // 它必须留在 pendingKlines 里等下一个间隔。
                    KlineBucketKey key = new KlineBucketKey(
                            kline.symbol(), kline.interval(), kline.bucketStart());
                    pendingKlines.remove(key, kline);
                    pendingAnchors.merge(new KlineKey(kline.symbol(), kline.interval()),
                            kline.bucketStart(), MarketDataServiceImpl::later);
                }
            }

            if (pendingAnchors.isEmpty()) {
                return;
            }
            List<KlineAnchor> anchors = new ArrayList<>(pendingAnchors.size());
            for (Map.Entry<KlineKey, Instant> entry : pendingAnchors.entrySet()) {
                Instant written = writtenAnchors.get(entry.getKey());
                if (written == null || entry.getValue().isAfter(written)) {
                    anchors.add(new KlineAnchor(entry.getKey().symbol(),
                            entry.getKey().interval(), entry.getValue()));
                }
            }
            if (anchors.isEmpty()) {
                // 待写锚点都已被更新过的锚点覆盖，无需重复覆盖同一行。
                pendingAnchors.clear();
                return;
            }
            repository.upsertAnchors(anchors);
            for (KlineAnchor anchor : anchors) {
                KlineKey key = new KlineKey(anchor.symbol(), anchor.interval());
                writtenAnchors.put(key, anchor.bucketStart());
            }
            // 锚点写入成功后清空待写标记；期间没有其它线程会修改 pendingAnchors。
            pendingAnchors.clear();
        }
    }

    private void flushSafely() {
        try {
            flushPending();
        } catch (RuntimeException ex) {
            // 快照仍留在 pendingKlines，下一个间隔继续重试；QuestDB 恢复后自动追上。
            log.warn("kline flush failed, {} snapshot(s) kept for retry: {}",
                    pendingKlines.size(), ex.toString());
        }
    }

    @Override
    public List<Kline> klines(String symbol, KlineInterval interval, int limit) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        return repository.findKlines(symbol, interval, limit);
    }

    @Override
    public List<Kline> latestKlines(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        List<Kline> latest = new ArrayList<>(aggregator.intervals().size());
        for (KlineInterval interval : aggregator.intervals()) {
            Kline current = aggregator.current(symbol, interval);
            if (current != null) {
                latest.add(current);
            }
        }
        return List.copyOf(latest);
    }

    @Override
    public List<TradeRecord> recentTrades(String symbol, int limit) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        // 只读内存缓存；limit 超过缓存容量时由缓存按容量截断。
        return tradeCache.recent(symbol, limit);
    }

    private static Instant later(Instant left, Instant right) {
        return left.isAfter(right) ? left : right;
    }

    private record KlineKey(String symbol, KlineInterval interval) {
    }

    private record KlineBucketKey(String symbol, KlineInterval interval, Instant bucketStart) {
    }

    /** 缓存未命中时恢复出的状态：当前 K 线（可能为 {@code null}）与库里的锚点 bucket。 */
    private record Restored(Kline current, Instant anchorBucket) {
    }

    /**
     * 一条批量 Kafka 消息内某个 (symbol, interval, bucket) 的水位判断身份。
     *
     * <p>带 bucket 是为了正确处理「一条批量消息跨 K 线周期」的重投：重投时当前快照可能已推进到
     * 后一个 bucket，必须逐 bucket 判断是否已聚合，不能只看 (partition, offset)。
     */
    private record KlineRecordKey(int partition, long offset, String symbol,
                                  KlineInterval interval, Instant bucketStart) {
    }
}
