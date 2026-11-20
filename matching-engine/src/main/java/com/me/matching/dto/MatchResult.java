package com.me.matching.dto;

import com.me.matching.entity.Trade;
import java.util.List;

/**
 * 一笔新委托的撮合结果。
 *
 * <p>限价单未成交的剩余量挂入订单簿。市价单按 IOC（Immediate-Or-Cancel）处理：
 * 剩余量不挂单，由委托模块在撮合结束后撤销，并退回剩余冻结资金（市价买入退回未用完的
 * 计价币预算，市价卖出退回未成交的基础币）。
 *
 * @param orderId           新委托 id
 * @param requestedQuantity 委托数量
 * @param filledQuantity    已成交量
 * @param remainingQuantity 未成交量（限价单已挂入订单簿；市价单已按 IOC 撤销）
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
