package com.me.matching.service.impl;

import com.me.matching.dto.MatchResult;
import com.me.matching.entity.BookOrder;
import com.me.matching.entity.Trade;
import com.me.matching.service.OrderBook;
import com.me.matching.vo.DepthLevel;
import com.me.matching.vo.DepthSnapshot;
import com.me.shared.money.Amounts;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * M0 集合版订单簿：{@code TreeMap} 价格梯 + {@code LinkedHashMap} FIFO。
 *
 * <h2>数据结构</h2>
 * <ul>
 *   <li><b>限价单</b>：{@code TreeMap<price, LinkedHashMap<orderId, BookOrder>>}。
 *       买盘价格降序、卖盘价格升序，TreeMap 提供价格优先；同一价位的 LinkedHashMap
 *       按插入顺序提供时间优先（FIFO）。</li>
 *   <li><b>市价单</b>：{@code LinkedHashMap<orderId, BookOrder>}，买卖各一，按到达顺序 FIFO。
 *       市价单没有价格，成交时用对手方限价单的价格。</li>
 * </ul>
 *
 * <h2>撮合规则</h2>
 * <ol>
 *   <li>新委托<b>先与对手方限价单撮合</b>：买吃卖盘最低价、卖吃买盘最高价；
 *       限价单价格不穿越时停止。</li>
 *   <li>仍有剩余且新委托是<b>限价单</b>时，<b>再与对手方市价单按 FIFO 撮合</b>。</li>
 *   <li><b>市价单不与市价单撮合</b>：市价新单只吃限价单；未成交部分通常挂到市价簿，
 *       等待后续限价单来吃。</li>
 *   <li><b>金额驱动的市价买入</b>：{@code frozenAmount > 0} 时视为即时单（IOC），
 *       每笔成交量还受剩余预算约束（预算 / 卖价），预算耗尽即停止，剩余量不挂单。</li>
 *   <li>每笔成交<b>以数量为基准</b>：成交量 = min(新单剩余量, 挂单剩余量)；
 *       金额 = 成交量 × 成交价。</li>
 * </ol>
 *
 * <h2>成交价</h2>
 * <ul>
 *   <li>限价 vs 限价：取挂单（maker）价格，价格优先、时间优先。</li>
 *   <li>限价 vs 市价：取限价单价格（市价单价格恒为 0）。</li>
 * </ul>
 *
 * <p><b>幂等</b>：冻结回执可能因 Kafka 事务重投或重复命令再次触发撮合，同一 orderId 只撮合一次，
 * 重复调用返回 {@link MatchResult#duplicate}。已见 orderId 保存在内存 Set 中，
 * 与订单簿一样由快照恢复机制负责清理（M0 不实现快照）。
 *
 * <p><b>线程模型</b>：撮合写入由委托模块在冻结回执监听线程中同步驱动（当前
 * {@code concurrency=1}，单线程顺序调用）；REST 深度查询会并发读取价格梯。M0 用同一个监视器锁
 * 保护 {@link #place} 和 {@link #depth}，保证快照一致性；读查询低频，锁开销可忽略。
 * 若将来提高消费并发，需要按 symbol 串行化后才能保持价格-时间优先。
 */
public final class CollectionOrderBook implements OrderBook {

    /** 单次深度查询最多返回的价位数量。 */
    public static final int MAX_DEPTH_LEVELS = 100;

    private final String symbol;

    /** 限价买盘：价格降序，最高买价在 firstKey()。 */
    private final NavigableMap<Long, LinkedHashMap<Long, BookOrder>> limitBids =
            new TreeMap<>(Comparator.reverseOrder());

    /** 限价卖盘：价格升序，最低卖价在 firstKey()。 */
    private final NavigableMap<Long, LinkedHashMap<Long, BookOrder>> limitAsks = new TreeMap<>();

    /** 市价买盘：按到达顺序 FIFO。 */
    private final Map<Long, BookOrder> marketBids = new LinkedHashMap<>();

    /** 市价卖盘：按到达顺序 FIFO。 */
    private final Map<Long, BookOrder> marketAsks = new LinkedHashMap<>();

    /** 已处理的 orderId，用于 at-least-once 去重。 */
    private final Set<Long> seenOrderIds = new HashSet<>();

    private long nextTradeId = 1L;

    public CollectionOrderBook(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        this.symbol = symbol;
    }

    @Override
    public String symbol() {
        return symbol;
    }

    @Override
    public synchronized MatchResult place(OrderPlacedEvent order) {
        validate(order);

        if (!seenOrderIds.add(order.orderId())) {
            return MatchResult.duplicate(order.orderId(), order.quantity());
        }

        boolean buy = order.side() == OrderSide.BUY;
        // 市价买入是金额驱动的即时单：frozenAmount 是计价币预算，撮合时按卖价折算可买数量，
        // 预算耗尽或没有可成交价位即结束，剩余数量不挂单（未用完的预算由委托模块退回）。
        MatchBudget budget = buy && order.type() == OrderType.MARKET && order.frozenAmount() > 0
                ? new MatchBudget(order.frozenAmount())
                : null;
        long remaining = order.quantity();
        List<Trade> trades = new ArrayList<>();

        // 1) 先与对手方限价单撮合：价格优先 + 同价 FIFO。
        remaining = matchLimitOrders(buy ? limitAsks : limitBids, order, remaining, trades, budget);

        // 2) 限价单仍有剩余时，再与对手方市价单 FIFO 撮合；市价单不参与这一步（不与市价撮合）。
        if (remaining > 0 && order.type() == OrderType.LIMIT) {
            remaining = matchMarketOrders(buy ? marketAsks : marketBids, order, remaining, trades);
        }

        // 3) 剩余量挂入本方订单簿；金额驱动的市价买入不挂单。
        if (remaining > 0 && budget == null) {
            rest(order, remaining);
        }
        return MatchResult.matched(order.orderId(), order.quantity(), remaining, trades);
    }

    /**
     * 与对手方限价价格梯撮合。买盘吃卖盘（价格从低到高），卖盘吃买盘（价格从高到低）；
     * 新委托是限价单时只吃到自己的限价为止，市价单则一路吃到没有可成交的限价单。
     */
    private long matchLimitOrders(NavigableMap<Long, LinkedHashMap<Long, BookOrder>> opposite,
                                  OrderPlacedEvent taker,
                                  long remaining,
                                  List<Trade> trades,
                                  MatchBudget budget) {
        Iterator<Map.Entry<Long, LinkedHashMap<Long, BookOrder>>> levels =
                opposite.entrySet().iterator();
        while (remaining > 0 && levels.hasNext()) {
            if (budget != null && budget.exhausted) {
                break;
            }
            Map.Entry<Long, LinkedHashMap<Long, BookOrder>> level = levels.next();
            long makerPrice = level.getKey();
            if (!crosses(taker, makerPrice)) {
                break;
            }

            LinkedHashMap<Long, BookOrder> makers = level.getValue();
            Iterator<BookOrder> makerIt = makers.values().iterator();
            while (remaining > 0 && makerIt.hasNext()) {
                if (budget != null && budget.exhausted) {
                    break;
                }
                BookOrder maker = makerIt.next();
                remaining = match(taker, maker, makerPrice, remaining, trades, budget);
                if (maker.filled()) {
                    makerIt.remove();
                }
            }
            if (makers.isEmpty()) {
                levels.remove();
            }
        }
        return remaining;
    }

    /**
     * 新委托是限价单、限价簿撮合后仍有剩余时，按 FIFO 吃对手方市价单。
     * 市价挂单价格为 0，成交价取新委托的限价。
     */
    private long matchMarketOrders(Map<Long, BookOrder> opposite,
                                   OrderPlacedEvent taker,
                                   long remaining,
                                   List<Trade> trades) {
        Iterator<BookOrder> makerIt = opposite.values().iterator();
        while (remaining > 0 && makerIt.hasNext()) {
            BookOrder maker = makerIt.next();
            remaining = match(taker, maker, taker.price(), remaining, trades, null);
            if (maker.filled()) {
                makerIt.remove();
            }
        }
        return remaining;
    }

    /**
     * 产生一笔成交：数量取双方剩余量的较小值，金额 = 数量 × 价格。
     * 金额驱动的市价买入还会按剩余预算限制成交量；预算不足以买入最小单位时标记
     * {@code exhausted}，外层据此停止撮合。
     * 先构造成交（含金额溢出校验），再扣减挂单，避免异常时订单簿出现半更新状态。
     */
    private long match(OrderPlacedEvent taker, BookOrder maker, long price,
                       long remaining, List<Trade> trades, MatchBudget budget) {
        long quantity = Math.min(remaining, maker.remainingQuantity());
        if (budget != null) {
            // 金额 / 卖价 = 可买数量；整除约束由 Amounts 保证，成交金额不会超过剩余预算。
            long affordable = Amounts.maxQuantityForBudget(budget.remaining, price,
                    taker.priceScale(), taker.quantityScale(), taker.quoteScale());
            if (affordable <= 0) {
                budget.exhausted = true;
                return remaining;
            }
            if (affordable < quantity) {
                quantity = affordable;
                budget.exhausted = true;
            }
        }
        Trade trade = Trade.matched(
                nextTradeId++,
                symbol,
                taker.orderId(),
                maker.orderId(),
                taker.userId(),
                maker.userId(),
                taker.side(),
                price,
                quantity,
                taker.priceScale(),
                taker.quantityScale(),
                taker.quoteScale());
        if (budget != null) {
            budget.remaining -= trade.amount();
        }
        maker.reduce(quantity);
        trades.add(trade);
        return remaining - quantity;
    }

    /** 金额驱动市价买入的剩余预算；{@code exhausted} 表示预算已买不起下一单位。 */
    private static final class MatchBudget {

        private long remaining;
        private boolean exhausted;

        private MatchBudget(long remaining) {
            this.remaining = remaining;
        }
    }

    /**
     * 新单是限价单时判断能否与 makerPrice 成交；市价单无价格约束。
     */
    private boolean crosses(OrderPlacedEvent taker, long makerPrice) {
        if (taker.type() == OrderType.MARKET) {
            return true;
        }
        return taker.side() == OrderSide.BUY
                ? makerPrice <= taker.price()
                : makerPrice >= taker.price();
    }

    /**
     * 剩余量挂入订单簿：限价单进 price -> LinkedHashMap，市价单进 LinkedHashMap。
     */
    private void rest(OrderPlacedEvent order, long remaining) {
        BookOrder bookOrder = new BookOrder(order, remaining);
        if (order.type() == OrderType.LIMIT) {
            limitBook(order.side())
                    .computeIfAbsent(order.price(), price -> new LinkedHashMap<>())
                    .put(order.orderId(), bookOrder);
        } else {
            marketBook(order.side()).put(order.orderId(), bookOrder);
        }
    }

    private NavigableMap<Long, LinkedHashMap<Long, BookOrder>> limitBook(OrderSide side) {
        return side == OrderSide.BUY ? limitBids : limitAsks;
    }

    private Map<Long, BookOrder> marketBook(OrderSide side) {
        return side == OrderSide.BUY ? marketBids : marketAsks;
    }

    @Override
    public int limitOrderCount(OrderSide side) {
        int count = 0;
        for (LinkedHashMap<Long, BookOrder> level : limitBook(side).values()) {
            count += level.size();
        }
        return count;
    }

    @Override
    public int marketOrderCount(OrderSide side) {
        return marketBook(side).size();
    }

    @Override
    public BookOrder find(long orderId) {
        for (BookOrder order : marketBids.values()) {
            if (order.orderId() == orderId) {
                return order;
            }
        }
        for (BookOrder order : marketAsks.values()) {
            if (order.orderId() == orderId) {
                return order;
            }
        }
        for (LinkedHashMap<Long, BookOrder> level : limitBids.values()) {
            BookOrder order = level.get(orderId);
            if (order != null) {
                return order;
            }
        }
        for (LinkedHashMap<Long, BookOrder> level : limitAsks.values()) {
            BookOrder order = level.get(orderId);
            if (order != null) {
                return order;
            }
        }
        return null;
    }

    @Override
    public synchronized DepthSnapshot depth(int levels) {
        int maxLevels = Math.max(1, Math.min(levels, MAX_DEPTH_LEVELS));
        List<DepthLevel> bids = aggregateLevels(limitBids, maxLevels);
        List<DepthLevel> asks = aggregateLevels(limitAsks, maxLevels);
        return new DepthSnapshot(
                symbol,
                bids,
                asks,
                bids.isEmpty() ? null : bids.get(0).price(),
                asks.isEmpty() ? null : asks.get(0).price(),
                marketBids.size(),
                marketAsks.size(),
                Instant.now());
    }

    /**
     * 按价格档位聚合剩余数量。买盘 TreeMap 是价格降序、卖盘是价格升序，
     * 因此返回列表首项就是买一 / 卖一。
     */
    private static List<DepthLevel> aggregateLevels(
            NavigableMap<Long, LinkedHashMap<Long, BookOrder>> book, int maxLevels) {
        List<DepthLevel> levels = new ArrayList<>(Math.min(maxLevels, book.size()));
        for (Map.Entry<Long, LinkedHashMap<Long, BookOrder>> entry : book.entrySet()) {
            if (levels.size() >= maxLevels) {
                break;
            }
            long quantity = 0;
            for (BookOrder order : entry.getValue().values()) {
                quantity += order.remainingQuantity();
            }
            levels.add(new DepthLevel(entry.getKey(), quantity, entry.getValue().size()));
        }
        return levels;
    }

    private void validate(OrderPlacedEvent order) {
        if (order == null) {
            throw new IllegalArgumentException("order must not be null");
        }
        if (!symbol.equals(order.symbol())) {
            throw new IllegalArgumentException(
                    "order symbol " + order.symbol() + " does not belong to book " + symbol);
        }
        if (order.orderId() <= 0) {
            throw new IllegalArgumentException("orderId must be positive: " + order.orderId());
        }
        if (order.userId() <= 0) {
            throw new IllegalArgumentException("userId must be positive: " + order.userId());
        }
        if (order.side() == null) {
            throw new IllegalArgumentException("order side is required");
        }
        if (order.type() == null) {
            throw new IllegalArgumentException("order type is required");
        }
        if (order.quantity() <= 0) {
            throw new IllegalArgumentException("quantity must be positive: " + order.quantity());
        }
        if (order.type() == OrderType.LIMIT && order.price() <= 0) {
            throw new IllegalArgumentException(
                    "limit order price must be positive: " + order.price());
        }
        if (order.type() == OrderType.MARKET && order.price() != 0) {
            throw new IllegalArgumentException("market order price must be 0: " + order.price());
        }
    }
}
