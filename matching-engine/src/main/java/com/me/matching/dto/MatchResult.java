package com.me.matching.dto;

import com.me.matching.entity.Trade;
import java.util.List;

/**
 * 一笔新委托的撮合结果。
 *
 * <p>未成交的剩余量通常会挂入订单簿：限价单挂限价簿，市价单挂市价簿。金额驱动的市价买入
 * （{@code frozenAmount > 0}）是即时单，剩余量不挂单，由委托模块标记完成并退回剩余预算。
 *
 * @param orderId           新委托 id
 * @param requestedQuantity 委托数量
 * @param filledQuantity    已成交量
 * @param remainingQuantity 未成交量（金额驱动的市价买入不挂单）
 * @param trades            本次撮合产生的成交，按撮合顺序排列
 * @param duplicate         同一 orderId 重复投递（at-least-once），本次未撮合
 */
public record MatchResult(
        long orderId,
        long requestedQuantity,
        long filledQuantity,
        long remainingQuantity,
        List<Trade> trades,
        boolean duplicate) {

    public MatchResult {
        trades = List.copyOf(trades);
    }

    public boolean fullyFilled() {
        return !duplicate && remainingQuantity == 0;
    }

    public static MatchResult matched(long orderId, long requestedQuantity, long remainingQuantity,
                                      List<Trade> trades) {
        return new MatchResult(orderId, requestedQuantity, requestedQuantity - remainingQuantity,
                remainingQuantity, trades, false);
    }

    public static MatchResult duplicate(long orderId, long requestedQuantity) {
        return new MatchResult(orderId, requestedQuantity, 0, requestedQuantity, List.of(), true);
    }
}
