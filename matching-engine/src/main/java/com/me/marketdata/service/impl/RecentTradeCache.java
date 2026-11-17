package com.me.marketdata.service.impl;

import com.me.marketdata.entity.TradeRecord;
import com.me.marketdata.repository.MarketDataRepository;
import com.me.marketdata.service.MarketDataService;
import com.me.shared.event.MatchPairEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 每个交易对的最新成交内存队列（容量 {@value #CAPACITY}，最新在前）。
 *
 * <p><b>写入</b>：行情消费批次落 QuestDB 之前，先把本批要归档的成交推进来；重投时按
 * {@code tradeId} 去重，因此重放不会在缓存里产生重复记录。
 *
 * <p><b>启动预热</b>：应用就绪后从 {@code md_trade} 查出已有交易对，各加载最新
 * {@value #CAPACITY} 条。QuestDB 尚未就绪时只记录告警，后续首次查询该交易对时再懒加载。
 *
 * <p><b>读取</b>：最新成交列表接口与盘口 WebSocket 推送都只读这份缓存，不再查询 QuestDB；
 * {@code limit} 超过容量按容量截断。同一交易对的读写由该交易对的监视器锁串行。
 */
@Component
public class RecentTradeCache {

    private static final Logger log = LoggerFactory.getLogger(RecentTradeCache.class);

    /** 每个交易对保留的成交条数；落库条数、接口与 WebSocket 推送共用该上限。 */
    static final int CAPACITY = MarketDataService.RECENT_TRADES_LIMIT;

    private final MarketDataRepository repository;
    private final ConcurrentHashMap<String, SymbolTrades> bySymbol = new ConcurrentHashMap<>();

    public RecentTradeCache(MarketDataRepository repository) {
        this.repository = repository;
    }

    /**
     * 启动预热：为成交表里出现过的每个交易对加载最新 {@link #CAPACITY} 条成交。
     *
     * <p>失败不阻止应用启动；缓存未命中时 {@link #recent(String, int)} 仍会懒加载。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        List<String> symbols;
        try {
            symbols = repository.findTradeSymbols();
        } catch (RuntimeException ex) {
            log.warn("recent trade cache warm-up skipped, cannot list symbols: {}", ex.toString());
            return;
        }
        for (String symbol : symbols) {
            try {
                SymbolTrades cache = bySymbol.computeIfAbsent(symbol, ignored -> new SymbolTrades());
                cache.ensureLoaded(() -> repository.findTrades(symbol, CAPACITY));
            } catch (RuntimeException ex) {
                log.warn("recent trade cache warm-up failed for {}: {}", symbol, ex.toString());
            }
        }
    }

    /**
     * 把一批成交放入对应交易对的队列（最新在前），并裁剪到容量上限。
     *
     * @param events 本批要归档的成交，按成交顺序（旧 → 新）
     */
    public void add(List<MatchPairEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        Map<String, List<MatchPairEvent>> bySymbolEvents = new LinkedHashMap<>();
        for (MatchPairEvent event : events) {
            if (event == null || event.symbol() == null || event.symbol().isBlank()) {
                continue;
            }
            bySymbolEvents.computeIfAbsent(event.symbol(), ignored -> new ArrayList<>()).add(event);
        }
        bySymbolEvents.forEach((symbol, symbolEvents) -> cache(symbol).add(symbolEvents));
    }

    /**
     * 查询某交易对缓存的成交，最新在前；{@code limit} 超过容量按容量截断。
     *
     * <p>缓存尚未加载（启动预热失败 / 预热之后才出现的交易对）时，从 {@code md_trade}
     * 读取最新 {@link #CAPACITY} 条后返回。
     */
    public List<TradeRecord> recent(String symbol, int limit) {
        SymbolTrades cache = cache(symbol);
        cache.ensureLoaded(() -> repository.findTrades(symbol, CAPACITY));
        return cache.snapshot(limit);
    }

    private SymbolTrades cache(String symbol) {
        return bySymbol.computeIfAbsent(symbol, ignored -> new SymbolTrades());
    }

    /**
     * 单个交易对的成交队列；所有读写都在实例锁内。
     *
     * <p>{@code trades} 头部最新、尾部最旧；{@code tradeIds} 与队列同步维护，用于重投去重。
     */
    private static final class SymbolTrades {

        private final ArrayDeque<TradeRecord> trades = new ArrayDeque<>(CAPACITY);
        private final Set<Long> tradeIds = new HashSet<>();
        private boolean loaded;

        /** 从持久化快照加载（最新在前）；已有缓存（消费线程先写入）时按 tradeId 合并。 */
        synchronized void ensureLoaded(Supplier<List<TradeRecord>> loader) {
            if (loaded) {
                return;
            }
            List<TradeRecord> stored = loader.get();
            // 逐条追加到队尾：stored 最新在前，因此队头仍是最新，历史按时间接在后面。
            for (TradeRecord trade : stored) {
                if (tradeIds.add(trade.tradeId())) {
                    trades.addLast(trade);
                }
            }
            loaded = true;
            trim();
        }

        /** 按成交顺序（旧 → 新）放入队头；重复的 tradeId 直接忽略。 */
        synchronized void add(List<MatchPairEvent> events) {
            for (MatchPairEvent event : events) {
                if (!tradeIds.add(event.tradeId())) {
                    continue;
                }
                trades.addFirst(new TradeRecord(
                        event.tradeId(),
                        event.symbol(),
                        event.price(),
                        event.quantity(),
                        event.amount(),
                        event.takerSide(),
                        event.executedAt()));
            }
            trim();
        }

        synchronized List<TradeRecord> snapshot(int limit) {
            int size = Math.min(Math.max(limit, 0), Math.min(trades.size(), CAPACITY));
            List<TradeRecord> result = new ArrayList<>(size);
            Iterator<TradeRecord> iterator = trades.iterator();
            while (iterator.hasNext() && result.size() < size) {
                result.add(iterator.next());
            }
            return List.copyOf(result);
        }

        private void trim() {
            while (trades.size() > CAPACITY) {
                TradeRecord removed = trades.removeLast();
                tradeIds.remove(removed.tradeId());
            }
        }
    }
}
