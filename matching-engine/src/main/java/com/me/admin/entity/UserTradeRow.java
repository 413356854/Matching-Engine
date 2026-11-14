package com.me.admin.entity;

import com.me.shared.order.OrderSide;
import com.me.shared.trade.UserTradeRole;
import java.time.Instant;

/**
 * QuestDB {@code user_trade} 表的查询结果行。
 *
 * <p>每个撮合对有两行（TAKER / MAKER）；{@code orderId} 是当前用户自己的委托，
 * {@code counterOrderId} 是成交对手的委托；{@code side} 是当前用户这一侧的方向。
 * 价格、数量、金额都是交易对最小单位的整数。
 */
public record UserTradeRow(
        long tradeId,
        String symbol,
        long userId,
        long orderId,
        UserTradeRole role,
        long counterOrderId,
        OrderSide side,
        long price,
        long quantity,
        long amount,
        Instant executedAt) {
}
