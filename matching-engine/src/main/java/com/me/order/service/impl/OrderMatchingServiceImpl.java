package com.me.order.service.impl;

import com.me.account.dto.TradeSettlementCommand;
import com.me.matching.dto.MatchResult;
import com.me.matching.entity.Trade;
import com.me.matching.service.MatchingEngine;
import com.me.order.dto.OrderMatchResult;
import com.me.order.dto.OrderMatchResult.IgnoredTrade;
import com.me.order.dto.OrderTradeApplyResult;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderService;
import com.me.shared.event.MarketDataEvent;
import com.me.shared.event.TradeEvent;
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
 * 委托侧撮合编排实现：进程内撮合 → 成交清算 → 生成行情/结算输出。
 *
 * <p><b>调用约束</b>：只由 {@code OrderFreezeResultListener} 调用；同一 symbol 的冻结回执
 * 由同一分区/consumer 串行处理。
 *
 * <p><b>演示版重投语义</b>：不做撮合结果缓存/重放。Kafka 事务正常提交时输出与 offset
 * 原子提交；事务回滚后的重投会再次调用 {@link MatchingEngine#place}，引擎按 orderId
 * 返回 duplicate，此时不再产生行情/结算（{@link OrderMatchResult.Outcome#DUPLICATE_IGNORED}）。
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
    public OrderMatchResult match(OrderPlacedEvent order, int sourcePartition, long sourceOffset,
                                  int sourceIndex) {
        MatchResult result = matchingEngine.place(order);
        if (result.duplicate()) {
            log.warn("matching duplicate ignored without replay: orderId={}, symbol={}, "
                            + "partition={}, offset={}, index={}",
                    order.orderId(), order.symbol(), sourcePartition, sourceOffset, sourceIndex);
            return OrderMatchResult.duplicateIgnored(order.orderId());
        }
        return buildOutputs(order, result, Instant.now());
    }

    private OrderMatchResult buildOutputs(OrderPlacedEvent order, MatchResult result,
                                          Instant executedAt) {
        List<MarketDataEvent> marketDataEvents = new ArrayList<>(result.trades().size());
        List<TradeSettlementCommand> settlementCommands =
                new ArrayList<>(result.trades().size() + 1);
        List<IgnoredTrade> ignoredTrades = new ArrayList<>();

        for (Trade trade : result.trades()) {
            marketDataEvents.add(toMarketData(trade, executedAt));

            OrderTradeApplyResult cleared = orderService.onTrade(toTradeEvent(trade, executedAt));
            if (cleared.handled()) {
                settlementCommands.addAll(cleared.settlementCommands());
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
            } else {
                log.warn("market buy completion ignored: orderId={}, reason={}",
                        order.orderId(), completed.reason());
            }
        }

        return OrderMatchResult.matched(result.orderId(),
                marketDataEvents, settlementCommands, ignoredTrades);
    }

    private static MarketDataEvent toMarketData(Trade trade, Instant executedAt) {
        return new MarketDataEvent(
                trade.tradeId(),
                trade.symbol(),
                trade.takerOrderId(),
                trade.makerOrderId(),
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
