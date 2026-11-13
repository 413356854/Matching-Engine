package com.me.order.dto;

import com.me.account.dto.TradeSettlementCommand;
import com.me.shared.order.OrderHistoryEvent;
import java.util.List;

/**
 * 成交对清算 / 市价买入完成结果。
 *
 * <p>{@code handled=true} 表示成交对已完成委托清算，{@code settlementCommands} 是需要投递到
 * 账户结算 topic 的<b>单用户</b>资金消息：买方一条、卖方一条。委托状态通过
 * {@code OrderService.view(orderId)} 查询；这里不再携带 {@code OrderView}。
 *
 * <p>{@code historyEvents} 是本次推进中进入终态的委托快照：taker / maker 谁被成交至
 * {@code FILLED}，就为谁生成一条历史事件；部分成交不产生历史事件。撤单产生的历史事件
 * 由 {@link OrderCancelApplyResult#historyEvent()} 携带。
 *
 * <p>{@code handled=false} 表示成交无法落到委托（委托不存在、状态不可成交、字段非法等），
 * 调用方只记录日志，不产生资金结算。
 */
public record OrderTradeApplyResult(
        boolean handled,
        String reason,
        List<TradeSettlementCommand> settlementCommands,
        List<OrderHistoryEvent> historyEvents) {

    public OrderTradeApplyResult {
        settlementCommands = settlementCommands == null ? List.of() : List.copyOf(settlementCommands);
        historyEvents = historyEvents == null ? List.of() : List.copyOf(historyEvents);
    }

    public static OrderTradeApplyResult handled(List<TradeSettlementCommand> settlementCommands,
                                                List<OrderHistoryEvent> historyEvents) {
        return new OrderTradeApplyResult(true, null, settlementCommands, historyEvents);
    }

    public static OrderTradeApplyResult handled(List<TradeSettlementCommand> settlementCommands) {
        return handled(settlementCommands, List.of());
    }

    public static OrderTradeApplyResult handled(TradeSettlementCommand settlementCommand) {
        return handled(List.of(settlementCommand), List.of());
    }

    public static OrderTradeApplyResult ignored(String reason) {
        return new OrderTradeApplyResult(false, reason, List.of(), List.of());
    }
}
