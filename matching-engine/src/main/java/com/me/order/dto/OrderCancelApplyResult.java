package com.me.order.dto;

import com.me.account.dto.TradeSettlementCommand;
import java.util.List;

/**
 * 撤单订单更新结果：把委托推进为 {@code CANCELED} 并生成解冻清算命令。
 *
 * <p>{@code handled=true} 表示本次调用完成了状态推进（或委托没有冻结资金可退），
 * {@code refundAmount} 是退回可用余额的金额（按 {@code frozenCurrency} 的最小单位）；
 * {@code settlementCommands} 是待投递到 {@code me.account.settlements-batch} 的单用户命令，
 * 一次撤单最多一条 {@code ORDER_UNFREEZE} 腿。
 *
 * <p>{@code handled=false} 表示委托不存在或状态已不再是可撤单状态（例如在撤单过程中
 * 全部成交、已被其它撤单命令撤销），本次不产生清算。
 */
public record OrderCancelApplyResult(
        boolean handled,
        String reason,
        long refundAmount,
        List<TradeSettlementCommand> settlementCommands) {

    public OrderCancelApplyResult {
        settlementCommands = settlementCommands == null ? List.of() : List.copyOf(settlementCommands);
    }

    public static OrderCancelApplyResult canceled(long refundAmount,
                                                  List<TradeSettlementCommand> settlementCommands) {
        return new OrderCancelApplyResult(true, null, refundAmount, settlementCommands);
    }

    public static OrderCancelApplyResult ignored(String reason) {
        return new OrderCancelApplyResult(false, reason, 0L, List.of());
    }
}
