package com.me.order.service.impl;

import com.me.account.dto.TradeSettlementCommand;
import com.me.matching.dto.CancelResult;
import com.me.matching.dto.MatchResult;
import com.me.matching.entity.Trade;
import com.me.matching.service.MatchingEngine;
import com.me.order.dto.OrderCancelBeginResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelMatchResult;
import com.me.order.dto.OrderMatchResult;
import com.me.order.dto.OrderMatchResult.IgnoredTrade;
import com.me.order.dto.OrderTradeApplyResult;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderService;
import com.me.shared.event.MatchPairEvent;
import com.me.shared.event.TradeEvent;
import com.me.shared.order.OrderCancelEvent;
import com.me.shared.order.OrderHistoryEvent;
import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 委托侧撮合编排实现：进程内撮合 → 成交清算 → 生成撮合对/结算输出。
 *
 * <p><b>调用约束</b>：下单由 {@code OrderFreezeResultListener} 调用，撤单由
 * {@code OrderCancelListener} 调用；两个入口来自不同 topic / consumer，可能并发。
 * 冻结回执的 {@link #matchBatch} 与 {@link #clear} 由监听器放在同一个引擎对象锁临界区内
 * （clear 分发到线程池执行），{@link #cancel} 使用同一把锁，因此下单撮合、成交清算、
 * 撤单移除与“撤单中”标记不会交错修改订单簿和委托状态。
 *
 * <p><b>演示版重投语义</b>：不做撮合结果缓存/重放。Kafka 事务正常提交时输出与 offset
 * 原子提交；事务回滚后的重投会再次调用 {@link MatchingEngine#place}，引擎按 orderId
 * 返回 duplicate，此时不再产生撮合对/结算（{@link OrderMatchResult.Outcome#DUPLICATE_IGNORED}）。
 * 这也意味着回滚重投可能丢失上一次未提交的输出——演示版接受该取舍，后续会补备份消息与
 * 内存状态恢复。
 *
 * <p>不要给本类加 {@code @Transactional}：Kafka 事务由监听容器管理，Spring 的 JDBC 事务注解
 * 只会增加语义歧义。
 */
@Service
public class OrderMatchingServiceImpl implements OrderMatchingService {

    private static final Logger log = LoggerFactory.getLogger(OrderMatchingServiceImpl.class);

    private final MatchingEngine matchingEngine;
    private final OrderService orderService;

    public OrderMatchingServiceImpl(MatchingEngine matchingEngine, OrderService orderService) {
        this.matchingEngine = matchingEngine;
        this.orderService = orderService;
    }

    @Override
    public List<MatchResult> matchBatch(List<OrderPlacedEvent> orders) {
        if (orders == null || orders.isEmpty()) {
            return List.of();
        }
        List<MatchResult> results = new ArrayList<>(orders.size());
        // 整批委托在同一个引擎对象锁临界区内按输入顺序撮合；已撤单中的委托不再挂单。
        synchronized (matchingEngine) {
            for (OrderPlacedEvent order : orders) {
                if (orderService.isCancelPending(order.orderId())) {
                    log.warn("matching skipped, order is being canceled: orderId={}, symbol={}",
                            order.orderId(), order.symbol());
                    results.add(MatchResult.duplicate(order.orderId(), order.quantity()));
                    continue;
                }
                results.add(matchingEngine.place(order));
            }
        }
        return results;
    }

    @Override
    public OrderMatchResult clear(OrderPlacedEvent order, MatchResult result) {
        if (order == null || result == null || result.duplicate()) {
            return OrderMatchResult.duplicateIgnored(order == null ? 0L : order.orderId());
        }
        return buildOutputs(order, result, Instant.now());
    }

    @Override
    public List<OrderCancelMatchResult> cancel(List<OrderCancelContext> contexts) {
        if (contexts == null || contexts.isEmpty()) {
            return List.of();
        }
        List<OrderCancelMatchResult> results = new ArrayList<>(contexts.size());
        // 引擎对象锁：与 place 共用同一把锁。整批撤消单在一个临界区内依次移除挂单并标记
        // “撤单中”，避免与冻结回执线程的下单撮合交错修改订单簿与委托状态。
        synchronized (matchingEngine) {
            for (OrderCancelContext context : contexts) {
                CancelResult engineResult = matchingEngine.cancel(new OrderCancelEvent(
                        context.orderId(), context.symbol(), context.side(),
                        context.type(), context.price()));
                OrderCancelBeginResult beginResult = orderService.beginCancel(context);
                results.add(new OrderCancelMatchResult(context, engineResult,
                        beginResult.accepted(), beginResult.reason()));
            }
        }
        return results;
    }

    private OrderMatchResult buildOutputs(OrderPlacedEvent order, MatchResult result,
                                          Instant executedAt) {
        List<MatchPairEvent> matchPairEvents = new ArrayList<>(result.trades().size());
        List<TradeSettlementCommand> settlementCommands =
                new ArrayList<>(result.trades().size() + 1);
        List<OrderHistoryEvent> historyEvents = new ArrayList<>(2);
        List<IgnoredTrade> ignoredTrades = new ArrayList<>();

        for (Trade trade : result.trades()) {
            matchPairEvents.add(toMatchPair(trade, executedAt));

            OrderTradeApplyResult cleared = orderService.onTrade(toTradeEvent(trade, executedAt));
            if (cleared.handled()) {
                settlementCommands.addAll(cleared.settlementCommands());
                historyEvents.addAll(cleared.historyEvents());
            } else {
                ignoredTrades.add(new IgnoredTrade(trade.tradeId(), cleared.reason()));
                log.warn("trade clearing ignored inside in-process matching: orderId={}, "
                                + "tradeId={}, takerOrderId={}, makerOrderId={}, reason={}",
                        result.orderId(), trade.tradeId(), trade.takerOrderId(),
                        trade.makerOrderId(), cleared.reason());
            }
        }

        // 市价买入按预算成交，撮合结束后不再挂单：标记委托完成，并把未用完的冻结预算
        // 生成一条退款清算命令。若最后成交已使委托全部成交，onTrade 已携带退款腿，
        // completeMarketBuy 会按状态幂等忽略。
        if (order.side() == OrderSide.BUY && order.type() == OrderType.MARKET) {
            OrderTradeApplyResult completed = orderService.completeMarketBuy(order.orderId());
            if (completed.handled()) {
                settlementCommands.addAll(completed.settlementCommands());
                historyEvents.addAll(completed.historyEvents());
            } else {
                log.warn("market buy completion ignored: orderId={}, reason={}",
                        order.orderId(), completed.reason());
            }
        }

        return OrderMatchResult.matched(result.orderId(),
                matchPairEvents, settlementCommands, historyEvents, ignoredTrades);
    }

    private static MatchPairEvent toMatchPair(Trade trade, Instant executedAt) {
        return new MatchPairEvent(
                trade.tradeId(),
                trade.symbol(),
                trade.takerOrderId(),
                trade.makerOrderId(),
                trade.takerUserId(),
                trade.makerUserId(),
                trade.takerSide(),
                trade.price(),
                trade.quantity(),
                trade.amount(),
                executedAt);
    }

    private static TradeEvent toTradeEvent(Trade trade, Instant executedAt) {
        return new TradeEvent(
                trade.tradeId(),
                trade.symbol(),
                trade.takerOrderId(),
                trade.makerOrderId(),
                trade.takerUserId(),
                trade.makerUserId(),
                trade.takerSide(),
                trade.price(),
                trade.quantity(),
                trade.amount(),
                executedAt);
    }
}
