package com.me.matching.service;

import com.me.matching.dto.CancelResult;
import com.me.matching.dto.MatchResult;
import com.me.matching.entity.BookOrder;
import com.me.matching.service.impl.CollectionOrderBook;
import com.me.matching.vo.DepthSnapshot;
import com.me.shared.order.OrderCancelEvent;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderType;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 撮合引擎入口：按交易对维护独立订单簿。
 *
 * <p>由委托模块在冻结回执流程中<b>进程内同步调用</b>（{@code OrderMatchingService}），
 * 不再消费 {@code me.orders}、也不再直接对接 Kafka。
 *
 * <p><b>引擎对象锁（TPS）</b>：{@link #place} 与 {@link #cancel} 都是 {@code synchronized}
 * 实例方法，多个冻结回执 / 撤单消费线程并发调用时，所有交易对的撮合写入严格串行。
 * 这保证了下单与撤单不会同时修改同一份订单簿状态；单个订单簿的深度查询仍由订单簿
 * 自己的监视器锁保护。压测口径下，引擎锁是撮合阶段的唯一全局串行点，因此消费并发再高
 * 也不会放大订单簿的锁竞争。
 *
 * <p>纯 JDK、零 Spring；装配为 Bean 由 {@code com.me.config.MatchingConfig} 负责。
 */
public final class MatchingEngine {

    private final ConcurrentHashMap<String, OrderBook> books = new ConcurrentHashMap<>();

    public synchronized MatchResult place(OrderPlacedEvent order) {
        if (order == null || order.symbol() == null || order.symbol().isBlank()) {
            throw new IllegalArgumentException("order and symbol are required");
        }
        return books.computeIfAbsent(order.symbol(), CollectionOrderBook::new).place(order);
    }

    /**
     * 撮合撤单：从对应交易对的订单簿移除挂单。
     *
     * <p>与 {@link #place} 使用同一把引擎对象锁；订单簿不存在或委托不在簿中时返回
     * {@link CancelResult#notFound}，委托模块仍然按委托成交进度解冻剩余冻结资金。
     */
    public synchronized CancelResult cancel(OrderCancelEvent cancel) {
        if (cancel == null || cancel.symbol() == null || cancel.symbol().isBlank()) {
            throw new IllegalArgumentException("cancel order and symbol are required");
        }
        if (cancel.orderId() <= 0) {
            throw new IllegalArgumentException(
                    "cancel orderId must be positive: " + cancel.orderId());
        }
        if (cancel.side() == null || cancel.type() == null) {
            throw new IllegalArgumentException("cancel order side and type are required");
        }
        if (cancel.type() == OrderType.LIMIT && cancel.price() <= 0) {
            throw new IllegalArgumentException(
                    "cancel limit order price must be positive: " + cancel.price());
        }
        if (cancel.type() == OrderType.MARKET && cancel.price() != 0) {
            throw new IllegalArgumentException(
                    "cancel market order price must be 0: " + cancel.price());
        }
        OrderBook book = books.get(cancel.symbol());
        if (book == null) {
            return CancelResult.notFound(cancel.orderId(), cancel.symbol());
        }
        BookOrder removed = book.cancel(cancel.orderId(), cancel.side(), cancel.type(),
                cancel.price());
        if (removed == null) {
            return CancelResult.notFound(cancel.orderId(), cancel.symbol());
        }
        return CancelResult.canceled(cancel.orderId(), cancel.symbol(),
                removed.remainingQuantity());
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
