package com.me.order.dto;

import com.me.account.dto.TradeSettlementCommand;
import com.me.shared.order.OrderHistoryEvent;
import java.util.List;

/**
 * 成交清算 / 市价单 IOC 收尾的结果：本次推进产生的结算命令与进入终态的委托历史。
 *
 * <p>成交由进程内撮合在引擎锁内同步产生并清算，taker / maker 必然已在委托表中且
 * 可成交，撤单也走同一把锁，因此清算没有失败结果；{@code settlementCommands} /
 * {@code historyEvents} 为空表示本次无需额外输出（例如市价单已在最后一笔成交中
 * 全部成交，退款腿已随成交结算携带）。
 */
public record OrderTradeApplyResult(
        List<TradeSettlementCommand> settlementCommands,
        List<OrderHistoryEvent> historyEvents) {

    public OrderTradeApplyResult {
        settlementCommands = settlementCommands == null ? List.of() : List.copyOf(settlementCommands);
        historyEvents = historyEvents == null ? List.of() : List.copyOf(historyEvents);
    }

    public static OrderTradeApplyResult of(List<TradeSettlementCommand> settlementCommands,
                                           List<OrderHistoryEvent> historyEvents) {
        return new OrderTradeApplyResult(settlementCommands, historyEvents);
    }

    public static OrderTradeApplyResult empty() {
        return new OrderTradeApplyResult(List.of(), List.of());
    }
}
