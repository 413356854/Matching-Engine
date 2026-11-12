package com.me.matching.service;

import com.me.matching.dto.MatchResult;
import com.me.matching.service.impl.CollectionOrderBook;
import com.me.matching.vo.DepthSnapshot;
import com.me.shared.order.OrderPlacedEvent;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 撮合引擎入口：按交易对维护独立订单簿。
 *
 * <p>由委托模块在冻结回执流程中<b>进程内同步调用</b>（{@code OrderMatchingService}），
 * 不再消费 {@code me.orders}、也不再直接对接 Kafka。监听容器当前 {@code concurrency=1}，
 * 同一交易对的委托按单一消费顺序进入引擎；将来若提高并发，需要按 symbol 串行化。
 *
 * <p>纯 JDK、零 Spring；装配为 Bean 由 {@code com.me.config.MatchingConfig} 负责。
 */
public final class MatchingEngine {

    private final ConcurrentHashMap<String, OrderBook> books = new ConcurrentHashMap<>();

    public MatchResult place(OrderPlacedEvent order) {
        if (order == null || order.symbol() == null || order.symbol().isBlank()) {
            throw new IllegalArgumentException("order and symbol are required");
        }
        return books.computeIfAbsent(order.symbol(), CollectionOrderBook::new).place(order);
    }

    /** 返回指定交易对的订单簿；尚未有委托时返回 {@code null}。 */
    public OrderBook book(String symbol) {
        return books.get(symbol);
    }

    /**
     * 返回指定交易对的订单簿深度快照；尚未有委托时返回空快照。
     *
     * @param levels 请求的价位数量，1..100，超出范围由订单簿夹取
     */
    public DepthSnapshot depth(String symbol, int levels) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        OrderBook book = books.get(symbol);
        return book == null ? DepthSnapshot.empty(symbol) : book.depth(levels);
    }

    public int symbolCount() {
        return books.size();
    }
}
