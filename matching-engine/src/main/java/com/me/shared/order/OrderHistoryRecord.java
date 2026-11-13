package com.me.shared.order;

import java.time.Instant;

/**
 * 委托历史查询结果（QuestDB {@code ord_history} 查询投影的只读快照）。
 *
 * <p>由 admin 模块的 QuestDB 访问层查询后映射；下单前端通过
 * {@link OrderHistoryQueryPort} 读取该契约，不直接访问 {@code ord_history} 表。
 *
 * <p>记录只包含撤单（{@code CANCELED}）与撮合完成（{@code FILLED}）两种终态，
 * 按 {@code finalAt} 倒序（最新在前）。
 */
public record OrderHistoryRecord(
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
