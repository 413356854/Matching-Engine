package com.me.admin.entity;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import java.time.Instant;

/**
 * QuestDB {@code ord_history} 表的查询结果行。
 */
public record OrderHistoryRow(
        long orderId,
        String requestId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        OrderStatus status,
        long price,
        long quantity,
        long filledQuantity,
        String frozenCurrency,
        long frozenAmount,
        int priceScale,
        int quantityScale,
        int quoteScale,
        long seq,
        Instant createdAt,
        Instant finalAt) {
}
