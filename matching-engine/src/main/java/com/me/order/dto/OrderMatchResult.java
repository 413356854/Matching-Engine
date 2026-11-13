package com.me.order.dto;

import com.me.account.dto.TradeSettlementCommand;
import com.me.shared.event.MarketDataEvent;
import com.me.shared.order.OrderHistoryEvent;
import java.util.List;

/**
 * 委托侧「进程内撮合 + 成交清算」的处理结果。
 *
 * <p>{@link Outcome#MATCHED} 表示首次撮合并生成了输出；{@link Outcome#DUPLICATE_IGNORED}
 * 表示撮合引擎按 orderId 返回 duplicate（重复回执/回滚重投），本次不产生输出。
 *
 * <p>{@code marketDataEvents} 对每笔成交都有一条（即使成交清算被忽略）；{@code settlementCommands}
 * 只包含清算成功（{@code handled=true}）的成交对；{@code historyEvents} 是本次清算中进入
 * {@code FILLED} 的 taker / maker 委托快照；{@code ignoredTrades} 记录无法清算的成交。
 */
public record OrderMatchResult(
        long orderId,
        Outcome outcome,
        List<MarketDataEvent> marketDataEvents,
        List<TradeSettlementCommand> settlementCommands,
        List<OrderHistoryEvent> historyEvents,
        List<IgnoredTrade> ignoredTrades) {

    public enum Outcome {
        MATCHED,
        DUPLICATE_IGNORED
    }

    /** 撮合成功但委托清算被忽略（例如 maker 委托不在委托模块内存中）。 */
    public record IgnoredTrade(long tradeId, String reason) {
    }

    public OrderMatchResult {
        marketDataEvents = List.copyOf(marketDataEvents);
        settlementCommands = List.copyOf(settlementCommands);
        historyEvents = List.copyOf(historyEvents);
        ignoredTrades = List.copyOf(ignoredTrades);
    }

    /** 是否需要把行情/结算/委托历史消息发布出去。 */
    public boolean publishable() {
        return outcome == Outcome.MATCHED;
    }

    public static OrderMatchResult matched(long orderId,
                                           List<MarketDataEvent> marketDataEvents,
                                           List<TradeSettlementCommand> settlementCommands,
                                           List<OrderHistoryEvent> historyEvents,
                                           List<IgnoredTrade> ignoredTrades) {
        return new OrderMatchResult(orderId, Outcome.MATCHED,
                marketDataEvents, settlementCommands, historyEvents, ignoredTrades);
    }

    public static OrderMatchResult duplicateIgnored(long orderId) {
        return new OrderMatchResult(orderId, Outcome.DUPLICATE_IGNORED,
                List.of(), List.of(), List.of(), List.of());
    }
}
