package com.me.websocket.service;

import com.me.marketdata.service.MarketDataService;
import com.me.marketdata.vo.KlineView;
import com.me.marketdata.vo.TradeView;
import com.me.matching.service.MatchingEngine;
import com.me.matching.vo.DepthSnapshot;
import com.me.websocket.service.OrderBookSubscriptionRegistry.Subscriber;
import com.me.websocket.vo.OrderBookMessage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

/**
 * 盘口定时推送：默认每 500ms 从撮合引擎读取一次订阅交易对的订单簿并推送。
 *
 * <p>同一条消息还附带该交易对最新 {@value MarketDataService#RECENT_TRADES_LIMIT} 笔成交
 * 与各配置周期当前正在聚合的最新 K 线（都读内存缓存，不查库）。成交与 K 线不单独订阅，
 * 随盘口快照一起下发；前端按 K 线时间戳覆盖最后一根或追加新柱子。
 *
 * <p>同一推送周期内，相同 {@code (symbol, depth)} 的订阅只查询引擎一次、只序列化一次，
 * 再发送给所有订阅该组合的连接；没有订阅时完全不访问引擎。
 */
@Component
public class OrderBookPushService {

    private static final Logger log = LoggerFactory.getLogger(OrderBookPushService.class);

    private final MatchingEngine matchingEngine;
    private final MarketDataService marketDataService;
    private final OrderBookSubscriptionRegistry registry;
    private final OrderBookMessageCodec codec;
    private final OrderBookSessionSender sender;

    public OrderBookPushService(
            MatchingEngine matchingEngine,
            MarketDataService marketDataService,
            OrderBookSubscriptionRegistry registry,
            OrderBookMessageCodec codec,
            OrderBookSessionSender sender) {
        this.matchingEngine = matchingEngine;
        this.marketDataService = marketDataService;
        this.registry = registry;
        this.codec = codec;
        this.sender = sender;
    }

    @Scheduled(fixedRateString = "${app.websocket.push-interval-ms:500}")
    public void pushSubscribedBooks() {
        List<Subscriber> subscribers = registry.subscribers();
        if (subscribers.isEmpty()) {
            return;
        }

        Map<BookKey, List<Subscriber>> groups = new LinkedHashMap<>();
        for (Subscriber subscriber : subscribers) {
            BookKey key = new BookKey(subscriber.symbol(), subscriber.depth());
            groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(subscriber);
        }

        for (Map.Entry<BookKey, List<Subscriber>> entry : groups.entrySet()) {
            BookKey key = entry.getKey();
            String payload;
            try {
                payload = codec.write(snapshotMessage(key.symbol(), key.depth()));
            } catch (RuntimeException ex) {
                // 某个交易对读取失败不影响其他交易对的推送。
                log.warn("failed to build order book snapshot for {}: {}",
                        key.symbol(), ex.toString());
                continue;
            }
            for (Subscriber subscriber : entry.getValue()) {
                sender.send(subscriber.session(), payload);
            }
        }
    }

    /** 订阅成功后的首帧：不必等下一个定时周期。 */
    public void pushNow(WebSocketSession session, String symbol, int depth) {
        try {
            sender.send(session, codec.write(snapshotMessage(symbol, depth)));
        } catch (RuntimeException ex) {
            log.warn("failed to push initial order book for {}: {}", symbol, ex.toString());
        }
    }

    /** 同一帧里同时构造盘口、最新成交与各周期最新 K 线；附加数据读取失败不影响盘口下发。 */
    private OrderBookMessage snapshotMessage(String symbol, int depth) {
        DepthSnapshot snapshot = matchingEngine.depth(symbol, depth);
        List<TradeView> trades;
        try {
            trades = marketDataService
                    .recentTrades(symbol, MarketDataService.RECENT_TRADES_LIMIT)
                    .stream()
                    .map(TradeView::from)
                    .toList();
        } catch (RuntimeException ex) {
            log.warn("failed to read recent trades for {}: {}", symbol, ex.toString());
            trades = List.of();
        }
        List<KlineView> klines;
        try {
            klines = marketDataService.latestKlines(symbol).stream()
                    .map(KlineView::from)
                    .toList();
        } catch (RuntimeException ex) {
            log.warn("failed to read latest klines for {}: {}", symbol, ex.toString());
            klines = List.of();
        }
        return OrderBookMessage.snapshot(snapshot, depth, trades, klines);
    }

    private record BookKey(String symbol, int depth) {
    }
}
