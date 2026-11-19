package com.me.order.dto;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import java.time.Instant;

/**
 * 按 requestId 查询到的当前内存委托快照。
 *
 * <p>与历史委托不同，这里不限制状态：{@code ACCEPTED} / {@code PAID} /
 * {@code PARTIALLY_FILLED} / {@code REJECTED} / {@code FILLED} / {@code CANCELED}
 * 都可能返回；{@code finalAt} 只在终态时非空。
 *
 * <p>由委托模块的 {@code OrderService.findByRequestId} 产出；跨模块调用方通过 shared
 * 契约或 REST 接口读取，不直接依赖委托模块的 {@code Order} 内部结构。
 */
public record OrderLookupRecord(
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
