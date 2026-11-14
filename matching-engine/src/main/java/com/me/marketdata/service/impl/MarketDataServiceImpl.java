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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 行情归档实现：按 Kafka poll 批次批量写成交明细，K 线按 {@code (symbol, interval)} 聚合后
 * 每个 bucket 只写最终快照。
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
 * 没有 K 线的历史区间不回溯生成。
 *
 * <p><b>成交明细</b>：每个 poll 批次只归档最后 {@value #TRADE_ARCHIVE_LIMIT} 条成交，
 * 避免 {@code md_trade} 随订单量线性膨胀；K 线仍然使用批内全部成交聚合。
 *
 * <p><b>K 线写入节流</b>：同一 {@code (symbol, interval, bucket)} 首次出现时立即写入，
 * 之后每 {@code app.marketdata.kline-write-every-batches} 个批次再更新一次；跨 bucket 时
 * 强制写入上一根的最终快照并补齐空 bucket。这样同一分钟的 K 线只有“首次 + 每 N 批 + 收尾”
 * 几次写入，而不是每批都写。
 *
 * <p><b>幂等与重启</b>：整批成交先批量写 {@code md_trade}（QuestDB DEDUP 去重），再在本地状态上
 * 计算各周期 K 线，最后批量写 {@code md_kline} 并推进锚点。K 线快照带
 * {@code (source_partition, source_offset)} 水位，重投时已包含的记录直接跳过。批量写成功后才
 * 提交内存状态，因此写库失败重试不会让内存领先于持久化结果；K 线写成功但锚点写失败时，
 * 重投会按锚点恢复，读到已落库的快照并跳过重复累计，同时补写锚点。
 *
 * <p>节流的代价是：当前分钟未到写入窗口时，最新累计状态只在内存中；进程此时崩溃，
 * 该窗口内已提交 Kafka offset 的成交不会重放，K 线会缺少这一小段。对实时快照口径可接受。
 */
@Service
public class MarketDataServiceImpl implements MarketDataService {

    /** 每个 poll 批次最多归档的成交明细条数；K 线仍聚合本批全部成交。 */
    static final int TRADE_ARCHIVE_LIMIT = 20;

    private final KlineAggregator aggregator;
    private final MarketDataRepository repository;
    private final int klineWriteEveryBatches;
    /**
     * 当前 bucket 自上次写入后经过的批次数；没有 key 表示该 bucket 还没写过。
     * marketdata 消费并发为 1，单线程访问；如果把该 stage 并发调大，需要改成线程安全容器
     * 或按分区拆状态。
     */
    private final Map<KlineBucketKey, Integer> batchesSinceKlineWrite = new ConcurrentHashMap<>();
    /** 自上次成功写入后又被更新过的 bucket；跨批次保留，用于判断收尾时是否需要强制写最终快照。 */
    private final Set<KlineBucketKey> dirtyBuckets = ConcurrentHashMap.newKeySet();

    @Autowired
    public MarketDataServiceImpl(KlineAggregator aggregator, MarketDataRepository repository,
                                 MarketDataProperties properties) {
        this(aggregator, repository, properties.klineWriteEveryBatches());
    }

    /** 测试用：直接指定 K 线写入间隔（1 = 每批都写）。 */
    MarketDataServiceImpl(KlineAggregator aggregator, MarketDataRepository repository,
                          int klineWriteEveryBatches) {
        if (klineWriteEveryBatches < 1) {
            throw new IllegalArgumentException(
                    "klineWriteEveryBatches must be >= 1: " + klineWriteEveryBatches);
        }
        this.aggregator = aggregator;
        this.repository = repository;
        this.klineWriteEveryBatches = klineWriteEveryBatches;
    }

    @Override
    public void clearWriteState() {
        batchesSinceKlineWrite.clear();
        dirtyBuckets.clear();
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

        // 只归档本批最后 20 条成交；K 线仍使用全部 records 聚合。
        // DEDUP 键保证整批重投不会产生重复成交。
        List<MatchPairEvent> trades =
                records.stream().map(MarketDataRecord::event).toList();
        int tradeFrom = Math.max(0, trades.size() - TRADE_ARCHIVE_LIMIT);
        repository.insertTrades(List.copyOf(trades.subList(tradeFrom, trades.size())));

        // 本批次内先在本地状态上计算，写库成功后再提交给 aggregator。
        Map<KlineKey, Kline> working = new HashMap<>();
        Set<KlineKey> resolved = new HashSet<>();
        // 每个 (symbol, interval) 在库里的锚点 bucket；null 表示还没有锚点记录。
        // 批次结束时只有 bucket 前进了（或首次建锚点）才写回，避免每批都覆盖同一行。
        Map<KlineKey, Instant> anchorBaselines = new HashMap<>();
        Map<KlineBucketKey, Kline> pending = new LinkedHashMap<>();
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
                    if (current != null) {
                        anchorBaselines.put(key, current.bucketStart());
                    } else {
                        Restored restored = restoreFromStore(key, bucketStart,
                                record.partition(), record.offset(), pending);
                        anchorBaselines.put(key, restored.anchorBucket());
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
                    Instant anchorBucket = anchorBaselines.get(key);
                    if (anchorBucket != null && bucketStart.isBefore(anchorBucket)) {
                        continue;
                    }
                } else if (current.bucketStart().isBefore(bucketStart)) {
                    // 当前 K 线早于事件 bucket：如果本 bucket 自上次写入后又被更新过，
                    // 把最终快照放进 pending（下面按“已完成 bucket”强制写入）；
                    // 否则它只是从库里恢复出来的旧快照，不需要重复写。
                    KlineBucketKey previousKey = new KlineBucketKey(
                            current.symbol(), interval, current.bucketStart());
                    if (dirtyBuckets.contains(previousKey)) {
                        pending.put(previousKey, current);
                    } else {
                        // 上一根已经是最新持久化状态，清理它的节流计数，避免跨分钟累积。
                        batchesSinceKlineWrite.remove(previousKey);
                    }
                    for (Kline flat : aggregator.flatsUntil(current, interval, bucketStart,
                            record.partition(), record.offset())) {
                        pending.put(new KlineBucketKey(
                                trade.symbol(), interval, flat.bucketStart()), flat);
                    }
                }

                Kline next = aggregator.next(current, trade, interval, bucketStart,
                        record.partition(), record.offset());
                if (next == null) {
                    continue;
                }
                working.put(key, next);
                dirtyBuckets.add(bucketKey);
                // 同一 bucket 只保留最终快照；批内中间状态对查询没有意义。
                pending.put(bucketKey, next);
            }
        }

        // 已完成 bucket（working 已经前进到更晚的 bucket）强制写最终快照；
        // 当前 bucket 首次立即写，之后每 N 批更新一次。
        List<Kline> toWrite = new ArrayList<>();
        for (Map.Entry<KlineBucketKey, Kline> entry : pending.entrySet()) {
            Kline kline = entry.getValue();
            Kline current = working.get(new KlineKey(kline.symbol(), kline.interval()));
            boolean finalSnapshot = current == null
                    || kline.bucketStart().isBefore(current.bucketStart());
            if (finalSnapshot) {
                toWrite.add(kline);
                batchesSinceKlineWrite.remove(entry.getKey());
                continue;
            }
            Integer written = batchesSinceKlineWrite.get(entry.getKey());
            if (written == null) {
                // 当前 bucket 的第一根快照：立即写入，保证查询能看到本分钟开盘。
                toWrite.add(kline);
                batchesSinceKlineWrite.put(entry.getKey(), 0);
            } else if (written + 1 >= klineWriteEveryBatches) {
                toWrite.add(kline);
                batchesSinceKlineWrite.put(entry.getKey(), 0);
            } else {
                batchesSinceKlineWrite.put(entry.getKey(), written + 1);
            }
        }
        if (!toWrite.isEmpty()) {
            repository.insertKlines(toWrite);
            for (Kline kline : toWrite) {
                dirtyBuckets.remove(new KlineBucketKey(
                        kline.symbol(), kline.interval(), kline.bucketStart()));
            }
        }
        // 只按已经持久化的 K 线推进锚点，避免锚点领先于快照：
        // 1) 本批真正写入的 bucket；
        // 2) 没有待写入更新（dirty）的当前 bucket——它要么是刚从库里恢复的，要么上一批已写过。
        Map<KlineKey, Instant> anchorBuckets = new HashMap<>();
        for (Kline kline : toWrite) {
            KlineKey key = new KlineKey(kline.symbol(), kline.interval());
            Instant existing = anchorBuckets.get(key);
            if (existing == null || kline.bucketStart().isAfter(existing)) {
                anchorBuckets.put(key, kline.bucketStart());
            }
        }
        for (Map.Entry<KlineKey, Kline> entry : working.entrySet()) {
            Kline kline = entry.getValue();
            KlineBucketKey bucketKey = new KlineBucketKey(
                    kline.symbol(), kline.interval(), kline.bucketStart());
            if (!dirtyBuckets.contains(bucketKey) && !anchorBuckets.containsKey(entry.getKey())) {
                anchorBuckets.put(entry.getKey(), kline.bucketStart());
            }
        }
        List<KlineAnchor> anchors = new ArrayList<>();
        for (Map.Entry<KlineKey, Instant> entry : anchorBuckets.entrySet()) {
            Instant baseline = anchorBaselines.get(entry.getKey());
            if (baseline == null || entry.getValue().isAfter(baseline)) {
                anchors.add(new KlineAnchor(entry.getKey().symbol(),
                        entry.getKey().interval(), entry.getValue()));
            }
        }
        if (!anchors.isEmpty()) {
            repository.upsertAnchors(anchors);
        }
        // 写库成功后才提交内存状态；失败时异常向上抛，aggregator 停在上一个成功批次。
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
     * 忽略。只有库里一根 K 线都没有时才是真正的新线。处理完成后会把当前 bucket 补写成锚点。
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
             cursor = cursor.plus(interval.span())) {
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

    @Override
    public List<Kline> klines(String symbol, KlineInterval interval, int limit) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        return repository.findKlines(symbol, interval, limit);
    }

    @Override
    public List<TradeRecord> recentTrades(String symbol, int limit) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        return repository.findTrades(symbol, limit);
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
