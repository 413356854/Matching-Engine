package com.me.order.dto;

import com.me.account.dto.TradeSettlementCommand;
import com.me.shared.event.MatchPairEvent;
import com.me.shared.order.OrderHistoryEvent;
import java.util.List;

/**
 * 委托侧「进程内撮合 + 成交清算」的处理结果。
 *
 * <p>{@link Outcome#MATCHED} 表示首次撮合并生成了输出；{@link Outcome#DUPLICATE_IGNORED}
 * 表示撮合引擎按 orderId 返回 duplicate（重复回执/回滚重投），本次不产生输出。
 *
 * <p>{@code matchPairEvents} 对每笔成交都有一条，且必然已同步生成结算命令；
 * {@code historyEvents} 是本次清算中进入终态的 taker / maker 委托快照（成交至
 * {@code FILLED}，或市价单 IOC 未成交部分进入 {@code CANCELED}）。
 */
public record OrderMatchResult(
        long orderId,
        Outcome outcome,
        List<MatchPairEvent> matchPairEvents,
        List<TradeSettlementCommand> settlementCommands,
        List<OrderHistoryEvent> historyEvents) {

    public enum Outcome {
        MATCHED,
        DUPLICATE_IGNORED
    }

    public OrderMatchResult {
        matchPairEvents = List.copyOf(matchPairEvents);
        settlementCommands = List.copyOf(settlementCommands);
        historyEvents = List.copyOf(historyEvents);
    }

    /** 是否需要把撮合对/结算/委托历史消息发布出去。 */
    public boolean publishable() {
        return outcome == Outcome.MATCHED;
    }

    public static OrderMatchResult matched(long orderId,
                                           List<MatchPairEvent> matchPairEvents,
                                           List<TradeSettlementCommand> settlementCommands,
                                           List<OrderHistoryEvent> historyEvents) {
        return new OrderMatchResult(orderId, Outcome.MATCHED,
                matchPairEvents, settlementCommands, historyEvents);
    }

    public static OrderMatchResult duplicateIgnored(long orderId) {
        return new OrderMatchResult(orderId, Outcome.DUPLICATE_IGNORED,
                List.of(), List.of(), List.of());
    }
}
